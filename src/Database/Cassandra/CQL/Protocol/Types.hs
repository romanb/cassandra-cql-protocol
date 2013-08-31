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

-- | The possibly supported compression algorithms. Which algorithms are
-- available depends on the Cassandra server version and configuration.
-- An 'Options' request message can be used to get a list of the supported
-- algorithms at runtime.
data Compression
    = Snappy
    | Zlib   -- ^ Cassandra 2.0+
    deriving (Eq, Show)

-- | The ID of a 'Prepare'd query used in each subsequent 'Execute' message.
newtype PreparedQueryId = PreparedQueryId ByteString deriving (Eq, Show)

type Keyspace = Text
type Table = Text

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
    | Execute !PreparedQueryId ![Maybe ByteString] !Consistency
        -- ^ Execute a prepared query with the given values for the previously
        -- bound placeholders and the desired consistency.
    | Register ![EventType]
        -- ^ Register for one or more 'Event's.
        -- A successful response contains a 'Ready' message.
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
        -- ^ Successful response to a 'Startup' or 'Register' message.
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
    | SchemaChange !SchemaChangeType !Keyspace !Table
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
        -- ^ Server error: something unexpected happened. This indicates a
        -- server-side bug.
    | ProtocolError
        -- ^ Protocol error: some client message triggered a protocol
        -- violation (for instance a 'Query' message is sent before a 'Startup'
        -- message has been sent).
    | BadCredentials
    | Unavailable
        { unavailConsistency :: !Consistency
            -- ^ The consistency level of the query having triggered
            -- the error.
        , unavailNumRequired :: !Int32
            -- ^ The number of nodes that are required to be alive to
            -- respect the desired consistency level of the query.
        , unavailNumAlive :: !Int32
            -- ^ The actual number of nodes that were known to be alive when
            -- the request was processed (alive < required).
        }
    | Overloaded
    | IsBootstrapping
    | TruncateError
    | WriteTimeout
        { wTimeoutConsistency :: !Consistency
            -- ^ The consistency level of the query having triggered the error.
        , wTimeoutNumAck :: !Int32
            -- ^ The number of nodes having acknowledged the write request.
        , wTimeoutNumRequired :: !Int32
            -- ^ The number of nodes that were required to acknowledge the
            -- write request in order for it to be successful.
        , wTimeoutWriteType :: !WriteType
        }
    | ReadTimeout
        { rTimeoutConsistency :: !Consistency
            -- ^ The consistency level of the query having triggered the error.
        , rTimeoutNumAck :: !Int32
            -- ^ The number of nodes having answered the read request.
        , rTimeoutNumRequired :: !Int32
            -- ^ The number of nodes that were required to answer the read request
            -- in order for it to be successful.
        , rTimeoutDataPresent :: !Bool
            -- ^ Whether the actual data was amongst the received responses.
            -- During reads, Cassandra doesn't request data from every replica
            -- to minimize internal network traffic. Instead, some replica are
            -- only asked for a checksum of the data. A read timeout may occur
            -- even if enough replica have responded to fulfill the consistency
            -- level if only checksum responses have been received. This flag
            -- allows to detect that case.
        }
    | SyntaxError
        -- ^ A submitted query has a syntax error.
    | Unauthorized
    | Invalid
        -- ^ A submitted query is syntactically correct but invalid due to
        -- other reasons.
    | ConfigError
        -- ^ A submitted query is invalid because of some configuration issue.
    | AlreadyExists !Keyspace !Table
        -- ^ A query attempted to create a keyspace and/or table that already
        -- exists.
    | Unprepared !PreparedQueryId
        -- ^ A prepared statement ID is not known to the host being queried.
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
