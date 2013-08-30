{-# LANGUAGE BangPatterns #-}
module Database.Cassandra.CQL.Protocol.Types where

import Data.ByteString (ByteString)
import Data.Int (Int8, Int32)
import Data.Text (Text)
import Data.UUID (UUID)
import Network.Socket (SockAddr)

-- Even though there is only a single supported version at this time,
-- since the protocol is designed to support multiple versions, so should this
-- implementation, so that the APIs are a bit more future-proof.
data CQLVersion = CQLv3 deriving (Eq, Show)

data Compression
    = Snappy
    | Zlib   -- ^ Cassandra 2.0+
    deriving (Eq, Show)

-- | The ID of a 'Prepare'd query.
newtype PreparedQueryId = PreparedQueryId ByteString deriving (Eq, Show)

data Request = Request
    { reqMessage :: !RequestMessage
        -- ^ The actual message of the request which determines the possible
        -- responses.
    , reqStreamId :: !Int8
        -- ^ The stream IDs is used to correlate async. responses to requests.
        -- Negative IDs are reserved for the server (e.g. all 'Event' responses
        -- have a stream ID of -1). Thus there are 127 stream IDs available for
        -- clients to manage async. requests per connection.
    , reqTracing :: !Bool
        -- ^ Whether to trace the request. If so, the response will contain a
        -- 'rspTraceId'. Note that not all requests support tracing.
        -- Currently, only 'Query', 'Prepare' and 'Execute' queries support tracing.
        -- Other requests will simply ignore this setting.
    } deriving (Eq, Show)

data RequestMessage
    = Startup !CQLVersion !(Maybe Compression)
        -- ^ Initialize a connection, optionally choosing a compression algorithm.
        -- This must be the first message of a connection, except for 'Options' that can
        -- be sent before to find out the options supported by the server. Once the
        -- connection has been initialized, a client should not send any more 'Startup'
        -- messages. A successful response contains a 'Ready' message.
    | Credentials ![(Text, Text)]
        -- ^ Send authentication credentials to the server.
        -- This message can be sent as a reaction to an 'Authenticate' message from the server,
        -- but can also be used later in the communication to change the authentication
        -- information. These key/value pairs are passed as is to the Cassandra
        -- IAuthenticator and thus the detail of which information is needed depends on
        -- that authenticator.
    | Options
        -- ^ Request supported options, like CQL versions and compression algorithms.
        -- A successful response contains a 'Supported' message.
    | Query !Text !Consistency
        -- ^ Directly execute a CQL query with the desired consistency.
        -- A successful response contains a 'Result' message with 'Rows'.
    | Prepare !Text
        -- ^ Prepare a CQL query for execution.
    | Execute
        { execPrepId :: !PreparedQueryId
            -- ^ The ID from the previously 'Prepared' query response.
        , execValues :: ![Maybe ByteString]
            -- ^ The values for the previously bound parameters (placeholders).
        , execConsistency :: !Consistency
            -- ^ The desired consistency guarantees of the query result.
        }
        -- ^ Execute a previously prepared CQL query.
    | Register ![EventType]
        -- ^ Register for one or more 'Event's.
    deriving (Eq, Show)

data Response = Response
    { rspMessage :: !ResponseMessage
    , rspStreamId :: !Int8
    , rspTraceId :: !(Maybe UUID)
    } deriving (Eq, Show)

data ResponseMessage
    = Authenticate !Text
        -- ^ Authentication challenge from the server, indicating the full class
        -- name of the IAuthenticator in use.
    | Ready
        -- ^ Successful response to a 'Startup' message.
    | Supported ![CQLVersion] ![Compression]
        -- ^ Successful response to an 'Options' message.
    | Result !Result
        -- ^ Successful response to a 'Query', 'Prepare' or 'Execute' message.
    | Event !Event
        -- ^ A push notification from the server, if the client previously
        -- 'Register'ed to any 'EventType's.
    | Error !Text !ErrorDetail
        -- ^ Any request can result in an 'Error' response from the server.
    deriving (Eq, Show)

-- | The possible results of 'Query', 'Prepare' or 'Execute' request messages.
data Result
    = Void
    | Rows !Metadata ![[Maybe ByteString]]
    | SetKeyspace !Text
    | Prepared !PreparedQueryId
    | SchemaChange !SchemaChangeType !Text !Text
    deriving (Eq, Show)

data Consistency
    = ANY
    | ONE
    | TWO
    | THREE
    | QUORUM
    | ALL
    | LOCAL_QUOROM
    | EACH_QUORUM
    deriving (Eq, Show)

data ErrorDetail
    = ServerError
    | ProtocolError
    | BadCredentials
    | Unavailable
        { unavailConsistency :: !Consistency
        , unavailNumRequired :: !Int32
        , unavailNumAlive :: !Int32
        }
    | Overloaded
    | IsBootstrapping
    | TruncateError
    | WriteTimeout
        { wTimeoutConsistency :: !Consistency
        , wTimeoutNumAck :: !Int32
        , wTimeoutNumRequired :: !Int32
        , wTimeoutWriteType :: !WriteType
        }
    | ReadTimeout
        { rTimeoutConsistency :: !Consistency
        , rTimeoutNumAck :: !Int32
        , rTimeoutNumRequired :: !Int32
        , rTimeoutDataPresent :: !Bool
        }
    | SyntaxError
    | Unauthorized
    | Invalid
    | ConfigError
    | AlreadyExists !Text !Text
    | Unprepared !ByteString
    deriving (Eq, Show)

data WriteType
    = SIMPLE
    | BATCH
    | BATCH_LOG
    | UNLOGGED_BATCH
    | COUNTER
    deriving (Eq, Show)

type Metadata = [ColumnInfo]

data ColumnInfo = ColumnInfo
    { colKeyspace :: !Text
    , colTable :: !Text
    , colName :: !Text
    , colType :: !ColumnType
    } deriving (Eq, Show)

-- | Cassandra data types.
data ColumnType
    = CCustom !Text
    | CAscii
    | CBigint
    | CBlob
    | CBoolean
    | CCounter
    | CDecimal
    | CDouble
    | CFloat
    | CInt
    | CText
    | CTimestamp
    | CUUID
    | CVarchar
    | CVarint
    | CTimeUUID
    | CInet
    | CList !ColumnType
    | CMap !ColumnType !ColumnType
    | CSet !ColumnType
    deriving (Eq, Show)

-- | Event types that clients can 'Register' to.
data EventType
    = TOPOLOGY_CHANGE
    | STATUS_CHANGE
    | SCHEMA_CHANGE
    deriving (Eq, Show)

-- | Events that clients can be notified of.
data Event
    = TopologyChanged !TopologyChangeType !SockAddr
        -- ^ A node was added or removed from the topology.
        -- To receive this event, 'Register' for 'TOPOLOGY_CHANGE'.
    | StatusChanged !StatusChangeType !SockAddr
        -- ^ The availability status of a node changed.
        -- To receive this event, 'Register' for 'STATUS_CHANGE'.
    | SchemaChanged SchemaChangeType !Text !Text
        -- ^ A schema change was made, mentioning the affected keyspace and table.
        -- If only a keyspace was affected, the table name will be empty.
        -- To receive this event, 'Register' for 'SCHEMA_CHANGE'.
    deriving (Eq, Show)

data TopologyChangeType
    = NEW_NODE
    | REMOVED_NODE
    deriving (Eq, Show)

data StatusChangeType
    = UP
    | DOWN
    deriving (Eq, Show)

data SchemaChangeType
    = CREATED
    | UPDATED
    | DROPPED
    deriving (Eq, Show)
