/**
 * Encryption utilities for securing migration space private keys
 * 
 * Uses AES-256-GCM for encryption with a key from environment variables.
 * This is temporary - keys will be removed after migration is complete.
 */
import crypto from 'crypto'
import { config } from '../config.js'

const ALGORITHM = 'aes-256-gcm'
const IV_LENGTH = 16

/**
 * Encrypt a private key for storage
 * 
 * @param {Uint8Array | string} privateKey - Private key to encrypt
 * @returns {string} - Encrypted data as JSON string
 */
export function encryptPrivateKey(privateKey) {
  if (!config.encryption.key) {
    throw new Error('MIGRATION_ENCRYPTION_KEY not configured')
  }
  
  // Convert to string if Uint8Array
  const keyString = typeof privateKey === 'string' 
    ? privateKey 
    : Buffer.from(privateKey).toString('base64')
  
  // Generate random IV
  const iv = crypto.randomBytes(IV_LENGTH)
  
  // Create cipher
  const cipher = crypto.createCipheriv(ALGORITHM, config.encryption.key, iv)
  
  // Encrypt
  const encrypted = Buffer.concat([
    cipher.update(keyString, 'utf8'),
    cipher.final()
  ])
  
  // Get authentication tag
  const authTag = cipher.getAuthTag()
  
  // Return as JSON string
  return JSON.stringify({
    encrypted: encrypted.toString('base64'),
    iv: iv.toString('base64'),
    authTag: authTag.toString('base64')
  })
}

/**
 * Decrypt a private key from storage
 * 
 * @param {string} encryptedData - Encrypted data as JSON string
 * @returns {string} - Decrypted private key
 */
export function decryptPrivateKey(encryptedData) {
  if (!config.encryption.key) {
    throw new Error('MIGRATION_ENCRYPTION_KEY not configured')
  }
  
  // Parse encrypted data
  const { encrypted, iv, authTag } = JSON.parse(encryptedData)
  
  // Create decipher
  const decipher = crypto.createDecipheriv(
    ALGORITHM,
    config.encryption.key,
    Buffer.from(iv, 'base64')
  )
  
  // Set authentication tag
  decipher.setAuthTag(Buffer.from(authTag, 'base64'))
  
  // Decrypt
  const decrypted = Buffer.concat([
    decipher.update(Buffer.from(encrypted, 'base64')),
    decipher.final()
  ]).toString('utf-8')
  
  return decrypted
}
