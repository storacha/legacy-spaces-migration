# Legacy Content Migration Flow

This diagram shows the complete migration workflow for legacy uploads that need space information added to their location claims and gateway authorizations.

```mermaid
sequenceDiagram
    participant Script as Migration Script
    participant Upload Table as Upload Table<br/>(DynamoDB)
    participant Consumer Table as Consumer Table<br/>(DynamoDB)
    participant Allocations Table as Allocations Table<br/>(DynamoDB)
    participant Store Table as Store Table<br/>(DynamoDB)
    participant Migration Spaces Table as Migration Spaces Table<br/>(DynamoDB)
    participant Indexing Service as Indexing Service
    participant Index Worker as Index Worker<br/>(HTTP API)
    participant Upload Service as Upload Service<br/>(UCAN)
    participant Content Claims as Content Claims<br/>(DynamoDB + S3)
    participant IPNI as IPNI Queue<br/>(SQS)
    participant Gateway as Gateway Service<br/>(UCAN)
    
    Note over Script,Gateway: STEP 0: Query Uploads Needing Migration
    Script->>Upload Table: Query uploads by space/customer
    Upload Table-->>Script: Upload records (root, space, shards[])
    
    opt Customer filtering
        Script->>Consumer Table: Get customer for space
        Consumer Table-->>Script: Customer DID
    end
    
    Note over Script,Gateway: STEP 1: Check Migration Status
    Script->>Indexing Service: Query claims for root CID
    Indexing Service-->>Script: Index claims + Location claims
    
    loop For each shard
        Script->>Script: Find location claims for shard
        Script->>Script: Check if claim has correct space
    end
    
    Script->>Script: Determine needed steps:<br/>- Index generation?<br/>- Location claims?<br/>- Gateway auth?
    
    alt Already Migrated
        Script->>Script: ✅ Skip - already complete
    else Needs Migration
        
        Note over Script,Gateway: STEP 2: Generate and Register Index
        alt Index Missing
            loop For each shard
                Script->>Allocations Table: Get shard size
                alt Not found
                    Script->>Store Table: Get shard size (fallback)
                end
            end
            
            Script->>Index Worker: POST /build-index<br/>(root, shards, sizes)
            Index Worker-->>Script: Index CAR bytes
            
            Script->>Consumer Table: Get customer for space
            Consumer Table-->>Script: Customer DID
            
            Script->>Migration Spaces Table: Get/create migration space
            Migration Spaces Table-->>Script: Migration space (encrypted key)
            
            Script->>Upload Service: space/blob/add<br/>(index blob to migration space)
            Upload Service-->>Script: Blob allocated
            
            Script->>Content Claims: Publish location claim<br/>(for index CAR itself)
            Content Claims-->>Script: Claim stored
            
            Script->>Upload Service: space/index/add<br/>(root → index CID)
            Upload Service-->>Script: Index registered
            
            Note right of Script: Index now queryable<br/>via indexing service
        else Index Exists
            Script->>Script: ⏭️ Skip index generation
        end
        
        Note over Script,Gateway: STEP 3: Republish Location Claims with Space
        alt Location Claims Missing Space
            loop For each shard needing space
                Script->>Script: Create assert/location claim<br/>with space DID in nb.space
                
                Script->>Content Claims: PUT claim (DynamoDB)
                Content Claims-->>Script: Claim stored
                
                Script->>Content Claims: PUT claim CAR (S3)
                Content Claims-->>Script: CAR stored
                
                Script->>IPNI: Publish location metadata<br/>(space + hash → contextID)
                IPNI-->>Script: Queued for indexing
            end
            
            Note right of Script: Indexing service will<br/>pick up new claims
        else Location Claims Have Space
            Script->>Script: ⏭️ Skip location claims
        end
        
        Note over Script,Gateway: STEP 4: Create Gateway Authorization
        alt Gateway Auth Missing
            Script->>Script: Create Absentee delegation:<br/>space → gateway<br/>(space/content/serve)
            
            Script->>Script: Create service attestation:<br/>upload-service attests<br/>Absentee delegation
            
            Script->>Script: Create access delegation:<br/>space → upload-service<br/>(access/*)
            
            Script->>Script: Create access attestation:<br/>upload-service attests<br/>access delegation
            
            Script->>Gateway: access/delegate invocation<br/>with: space DID<br/>nb.delegations: {content/serve}<br/>proofs: [access, attestations]
            Gateway->>Gateway: Validate attestations<br/>using validator proofs
            Gateway->>Gateway: Store delegation in KV:<br/>${space}:${delegationCID}
            Gateway-->>Script: Authorization stored
            
            Note right of Gateway: Gateway can now serve<br/>content with egress tracking
        else Gateway Auth Exists
            Script->>Script: ⏭️ Skip gateway auth
        end
        
        Note over Script,Gateway: STEP 5: Verify Migration
        Script->>Indexing Service: Re-query index claim
        Indexing Service-->>Script: Index claim verified
        
        loop For each shard
            Script->>Indexing Service: Re-query location claims
            Indexing Service-->>Script: Location claims with space
            
            alt Space Missing in Indexer
                Script->>Content Claims: Fallback: Query DynamoDB directly
                Content Claims-->>Script: Claims with space info
                
                Script->>Content Claims: Fetch claim CAR from S3
                Content Claims-->>Script: Parse CBOR blocks
                
                Script->>Script: Verify space in capability.nb.space
            end
        end
        
        Script->>Script: Verify gateway auth result
        
        alt All Checks Pass
            Script->>Script: ✅ Migration complete
        else Verification Failed
            Script->>Script: ❌ Migration failed<br/>(log details)
        end
    end
    
    Note over Script,Gateway: Migration Complete - Content Ready for Egress Tracking
```

## Key Components

### Data Structures

**Upload Record (DynamoDB)**
```javascript
{
  space: "did:key:z6Mk...",
  root: "bafkreie...",
  shards: ["bagbaiera...", "bagbaiera..."]
}
```

**Location Claim (with space)**
```javascript
{
  can: "assert/location",
  nb: {
    content: { digest: <multihash> },
    location: [<multiaddr>],
    space: "did:key:z6Mk..."  // ← Added by migration
  }
}
```

**Gateway Authorization**
```javascript
// Absentee delegation (no private key)
{
  issuer: Absentee("did:key:z6Mk..."),
  audience: "did:web:w3s.link",
  capabilities: [{
    can: "space/content/serve",
    with: "did:key:z6Mk..."
  }],
  proofs: [attestation]  // Attested by upload-service
}
```

## Migration States

1. **Not Started**: No index, no space in claims, no gateway auth
2. **Index Only**: Index exists, but location claims missing space
3. **Claims Only**: Location claims have space, but no gateway auth
4. **Complete**: Index + location claims with space + gateway auth

## Test Modes

- `--test-index`: Only generate and register index
- `--test-location-claims`: Only republish location claims
- `--test-gateway-auth`: Only create gateway authorization
- `--verify-only`: Check status without making changes
- (no flag): Full migration (all steps)

## Verification

The verification step ensures:
1. ✅ Index claim exists in indexing service
2. ✅ Location claims exist for all shards
3. ✅ Location claims include space information
4. ✅ Gateway authorization succeeded (based on step result)

If indexing service doesn't show space info yet (propagation delay), falls back to querying DynamoDB + S3 directly.
