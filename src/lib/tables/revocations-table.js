import {
  DynamoDBClient,
  BatchGetItemCommand,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand
} from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import { parseLink } from '@ucanto/core'
import { config } from '../../config.js'

/**
 * @typedef {import('@ucanto/interface').UCANLink} UCANLink
 * @typedef {import('@ucanto/interface').DID} DID
 */

/**
 * Revocation record
 * @typedef {Object} Revocation
 * @property {UCANLink} revoke - The revoked delegation CID
 * @property {DID} scope - The DID of the authority that issued the revocation
 * @property {UCANLink} cause - The ucan/revoke invocation that authorized the revocation
 */

/**
 * Query for revocations
 * @typedef {Record<string, {}>} RevocationQuery
 */

/**
 * Matching revocations result
 * @typedef {Record<string, Record<DID, { cause: UCANLink }>>} MatchingRevocations
 */

const REVOCATIONS_TABLE_NAME = process.env.REVOCATION_TABLE_NAME || 'staging-w3infra-revocation'

// Static keys that are part of the schema (not scope DIDs)
const staticRevocationKeys = new Set(['revoke'])

/**
 * Create a revocations table client
 * 
 * @param {object} [options]
 * @param {string} [options.tableName] - DynamoDB table name
 * @param {string} [options.region] - AWS region
 * @param {string} [options.endpoint] - DynamoDB endpoint (for local testing)
 * @returns {Promise<RevocationsStorage>}
 */
export async function createRevocationsTable(options = {}) {
  const tableName = options.tableName || REVOCATIONS_TABLE_NAME
  const region = options.region || config.aws.region
  
  const credentials = config.aws.accessKeyId && config.aws.secretAccessKey
    ? {
        accessKeyId: config.aws.accessKeyId,
        secretAccessKey: config.aws.secretAccessKey,
      }
    : undefined

  const dynamoDb = new DynamoDBClient({
    region,
    credentials,
    ...(options.endpoint && { endpoint: options.endpoint }),
  })

  return useRevocationsTable(dynamoDb, tableName)
}

/**
 * @typedef {Object} RevocationsStorage
 * @property {(revocation: Revocation) => Promise<{ ok?: {}, error?: Error }>} add
 * @property {(revocation: Revocation) => Promise<{ ok?: {}, error?: Error }>} reset
 * @property {(query: RevocationQuery) => Promise<{ ok?: MatchingRevocations, error?: Error }>} query
 */

/**
 * Create revocations storage interface
 * 
 * @param {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamoDb
 * @param {string} tableName
 * @returns {RevocationsStorage}
 */
function useRevocationsTable(dynamoDb, tableName) {
  return {
    /**
     * Add a revocation to the store
     * @param {Revocation} revocation
     */
    async add(revocation) {
      try {
        await dynamoDb.send(new UpdateItemCommand({
          TableName: tableName,
          Key: marshall({
            revoke: revocation.revoke.toString(),
          }),
          // When we get a new revocation, this update expression will create a new "column"
          // in the table row that is keyed by "revokeCID". The name of this new column will be
          // the "scopeCID" and the value will be a map containing metadata - currently just the
          // causeCID
          UpdateExpression: 'SET #scope = :scopeMetadata',
          ExpressionAttributeNames: {
            '#scope': revocation.scope.toString()
          },
          ExpressionAttributeValues: marshall({
            ':scopeMetadata': { cause: revocation.cause.toString() },
          })
        }))
        return { ok: {} }
      } catch (error) {
        return { error: /** @type {Error} */ (error) }
      }
    },

    /**
     * Reset revocations for a delegation (replaces all existing revocations)
     * @param {Revocation} revocation
     */
    async reset(revocation) {
      try {
        await dynamoDb.send(new PutItemCommand({
          TableName: tableName,
          Item: marshall({
            revoke: revocation.revoke.toString(),
            [revocation.scope.toString()]: {
              cause: revocation.cause.toString()
            }
          })
        }))
        return { ok: {} }
      } catch (error) {
        return { error: /** @type {Error} */ (error) }
      }
    },

    /**
     * Remove a revocation
     * @param {UCANLink} revokeCID - The delegation CID to remove revocation for
     */
    async remove(revokeCID) {
      try {
        await dynamoDb.send(new DeleteItemCommand({
          TableName: tableName,
          Key: marshall({
            revoke: revokeCID.toString()
          })
        }))
        return { ok: {} }
      } catch (error) {
        return { error: /** @type {Error} */ (error) }
      }
    },

    /**
     * Query for revocations
     * @param {RevocationQuery} query
     */
    async query(query) {
      try {
        const delegationCIDs = Object.keys(query)
        
        // BatchGetItem only supports batches of 100 and return values under 16MB
        if (delegationCIDs.length > 100) {
          throw new Error('checking for more than 100 revocations in a single call is currently not supported')
        }
        
        if (delegationCIDs.length === 0) {
          return { ok: {} }
        }

        const result = await dynamoDb.send(new BatchGetItemCommand({
          RequestItems: {
            [tableName]: {
              Keys: delegationCIDs.map(cid => marshall({ revoke: cid.toString() })),
            }
          }
        }))

        if (!result.Responses) {
          throw new Error('Did not receive a response from DynamoDB')
        }
        
        if (result.UnprocessedKeys && (Object.keys(result.UnprocessedKeys).length > 0)) {
          throw new Error('Dynamo did not process all keys')
        }

        const revocations = result.Responses?.[tableName].reduce((m, marshalledItem) => {
          const item = unmarshall(marshalledItem)
          const revokeCID = /** @type {UCANLink} */(parseLink(item.revoke))
          
          for (const [key, value] of Object.entries(item)) {
            // all values other than those explicitly listed in the schema are assumed
            // to be map values keyed by scopeDID
            if (!staticRevocationKeys.has(key)) {
              const revokeCIDStr = /** @type {string} */(revokeCID.toString())
              const scopeDID = /** @type {DID} */(key)
              m[revokeCIDStr] ||= {}
              m[revokeCIDStr][scopeDID] = {
                cause: parseLink(value.cause)
              }
            }
          }
          return m
        }, /** @type {MatchingRevocations} */({}))
        
        return { ok: revocations }
      } catch (error) {
        return { error: /** @type {Error} */ (error) }
      }
    }
  }
}
