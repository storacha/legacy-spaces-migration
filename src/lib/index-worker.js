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
 * @param {import('multiformats').CID} shardCID - The CID of the shard
 * @param {number} size - The size of the blob in bytes
 * @returns {Promise<Map<Uint8Array, [number, number]>>} - Map of digest bytes to [offset, length]
 */
async function buildIndex(shardCID, size) {
  const slices = new Map()
  
  let blocks = 0
  let offset = 0
  let requestCount = 0
  let consecutiveRetries = 0
  const MAX_RETRIES = 3
  
  // Try multiple formats:
  // 1. CID string (e.g., bagbaiera... for CIDv1)
  // 2. Base58btc encoded multihash (e.g., zQm... for legacy CIDv0-style)
  const cidString = shardCID.toString()
  const multihashB58 = base58btc.encode(shardCID.multihash.bytes)
  const identifiers = [cidString, multihashB58]
  
  // Try both .car (legacy) and .blob (new) extensions
  const extensions = ['.car', '.blob']
  
  let blobKey = null
  let lastError = null
  
  // Find which format exists by trying the first offset
  for (const id of identifiers) {
    for (const ext of extensions) {
      const testKey = `${id}/${id}${ext}`
      try {
        const url = `${WORKER_URL}/index/${testKey}?offset=0`
        const res = await fetch(url, { method: 'HEAD' })
        if (res.ok) {
          blobKey = testKey
          break
        }
      } catch (err) {
        lastError = err
      }
    }
    if (blobKey) break
  }
  
  if (!blobKey) {
    throw new Error(`Blob not found in carpark (tried CID and multihash formats with .car and .blob): ${cidString}`, { cause: lastError })
  }
  
  while (offset < size) {
    const prevBlocks = blocks
    
    try {
      requestCount++
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
      
      // Success - reset retry counter
      consecutiveRetries = 0
      
    } catch (err) {
      // If we didn't index any new blocks, this is a fatal error
      if (prevBlocks === blocks) {
        const errorMsg = err instanceof Error ? err.message : String(err)
        throw new Error(`Failed to index ${blobKey}: ${errorMsg}`, { cause: err })
      }
      
      // Otherwise, we indexed some blocks before the error, so we can retry
      consecutiveRetries++
      if (consecutiveRetries > MAX_RETRIES) {
        throw new Error(`Max retries (${MAX_RETRIES}) exceeded for ${blobKey} at offset ${offset}`, { cause: err })
      }
      console.warn(`Retrying after partial failure (attempt ${consecutiveRetries}/${MAX_RETRIES}): ${err.message}`)
    }
  }
  return { slices, requestCount }
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
  console.log(`Generating sharded index for ${rootCID} with ${shards.length} shards...`)
  const index = ShardedDAGIndex.create(root)
  
  // Build indices for all shards in parallel since it's remote work
  const shardIndexPromises = shards.map(async (shard) => {
    const shardCID = CID.parse(shard.cid)
    
    // Generate index for the shard using the worker (tries different formats)
    const { slices, requestCount } = await buildIndex(shardCID, shard.size)
    
    return { shardCID, slices, requestCount }
  })
  
  const shardIndices = await Promise.all(shardIndexPromises)
  
  // Calculate total requests
  const totalRequests = shardIndices.reduce((sum, { requestCount }) => sum + requestCount, 0)
  
  // Add all slices to the index
  for (const { shardCID, slices } of shardIndices) {
    const shardSize = shards.find(s => s.cid === shardCID.toString())?.size
    
    // Add the shard itself as a slice FIRST (full CAR file)
    if (shardSize) {
      index.setSlice(shardCID.multihash, shardCID.multihash, { offset: 0, length: shardSize })
    }
    
    // Then add content slices
    for (const [digestBytes, position] of slices.entries()) {
      const sliceDigest = Digest.decode(digestBytes)
      const [offset, length] = position
      index.setSlice(shardCID.multihash, sliceDigest, { offset, length })
    }
  }
  
  const archiveResult = await index.archive()
  if (archiveResult.error) {
    throw new Error('Failed to archive index', { cause: archiveResult.error })
  }

  return {
    indexBytes: archiveResult.ok,
    shardIndices,
    totalRequests,
  }
}
