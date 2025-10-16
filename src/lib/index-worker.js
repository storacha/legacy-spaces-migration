/**
 * Index Worker integration for generating sharded DAG indices
 * without downloading CAR files from S3/R2
 */

import { Parse } from 'ndjson-web'
import * as dagJSON from '@ipld/dag-json'
import { ShardedDAGIndex } from '@storacha/blob-index'
import { base58btc } from 'multiformats/bases/base58'
import { CID } from 'multiformats/cid'
import * as Digest from 'multiformats/hashes/digest'

const WORKER_URL = 'https://index-worker-carpark-production.protocol-labs.workers.dev'

/**
 * Generates the index for a blob using the Index Worker
 * 
 * @param {string} blobKey - The key of the blob to index (e.g. "<multihash>/<multihash>.blob")
 * @param {number} size - The size of the blob in bytes
 * @returns {Promise<Map<Uint8Array, [number, number]>>} - Map of digest bytes to [offset, length]
 */
export async function buildIndex(blobKey, size) {
  const slices = new Map()
  
  let blocks = 0
  let offset = 0
  
  while (offset < size) {
    const prevBlocks = blocks
    
    try {
      const url = `${WORKER_URL}/index/${blobKey}?offset=${offset}`
      console.log(`Building index for ${blobKey}?offset=${offset}...`)
      const res = await fetch(url)
      if (!res.ok) {
        throw new Error(`Worker returned ${res.status}: ${await res.text()}`)
      }
      
      await res.body
        .pipeThrough(new Parse(line => dagJSON.parse(line)))
        .pipeTo(new WritableStream({
          write([digestBytes, position]) {
            blocks++
            slices.set(digestBytes, position)
            offset = position[0] + position[1]
          }
        }))
      
      // If no more blocks were indexed, the CAR must be corrupt
      if (prevBlocks === blocks) {
        throw new Error('server sent no more blocks even though block offset is less than CAR size')
      }
      
    } catch (err) {
      // If we didn't index any new blocks, this is a fatal error
      if (prevBlocks === blocks) {
        throw new Error(`Failed to index ${key}`, { cause: err })
      }
      // Otherwise, we indexed some blocks before the error, so we can retry
    }
  }
  console.log(`Built index for ${blobKey} with ${blocks} blocks`)
  return slices
}

/**
 * Generate a complete sharded DAG index for an upload using the index worker
 * 
 * @param {string} rootCID - Root CID string
 * @param {Array<{cid: string, size: number}>} shards - Array of shard info
 * @returns {Promise<Uint8Array>} - Archived index as CAR bytes
 */
export async function generateShardedIndex(rootCID, shards) {
  const root = CID.parse(rootCID)
  const index = ShardedDAGIndex.create(root)
  for (const shard of shards) {
    const shardCID = CID.parse(shard.cid)
    
    // Convert CID to blob key format: <multihash>/<multihash>.blob
    const shardMultihash = base58btc.encode(shardCID.multihash.bytes)
    const blobKey = `${shardMultihash}/${shardMultihash}.blob`
    
    // Generate index for the shard using the worker
    const slices = await buildIndex(blobKey, shard.size)
    
    // Add all slices to the index
    for (const [digestBytes, position] of slices.entries()) {
      const sliceDigest = Digest.decode(digestBytes)
      index.setSlice(shardCID.multihash, sliceDigest, position)
    }
  }
  
  const archiveResult = await index.archive()
  if (archiveResult.error) {
    throw new Error('Failed to archive index', { cause: archiveResult.error })
  }
  
  return archiveResult.ok
}
