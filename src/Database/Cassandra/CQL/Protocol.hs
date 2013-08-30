{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-|
An implementation of the Cassandra CQL binary protocol (currently v1).

This is an implementation of the CQL binary protocol that builds on modified parts
of an existing implementation of the protocol that is built into the 'cassandra-cql'
package, which provides an all-in-one Cassandra CQL client.
In contrast, the purpose of this library is to have a reusable, standalone and
complete implementation of the protocol that leaves any other concerns, like the
actual I/O, connection management/pooling, sync vs async queries or convenient,
high-level ways of defining and constructing the actual CQL queries, to user code
and/or other client libraries that are built on top of this protocol implementation.

Reference: <https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v1.spec>
-}
module Database.Cassandra.CQL.Protocol
    ( -- * Data Types
      module Database.Cassandra.CQL.Protocol.Types
    , Blob(..)
    , Counter(..)
    , Timestamp(..)
    , TimeUUID(..)

      -- * Request & Response Encoding and Decoding
    , encodeRequest
    , decodeResponse
      -- ** Compression
      -- $compression
    , Compress
    , Decompress

      -- * Encoding & Decoding Values
    , FromCasValue(..)
    , ToCasValue(..)
    , FromCasValues(..)
    , ToCasValues(..)
    , Param(..)
    , encodeValue
    , decodeValue
    , decodeRows

      -- * Request Construction
    , simpleRequest
    , startupRequest
    , simpleQuery
    ) where

import Database.Cassandra.CQL.Protocol.Types

import Control.Arrow (left)
import Control.Applicative ((<$>), (<*>))
import Control.Monad (forM_, replicateM, foldM)
import Data.Bits ((.&.), (.|.), shiftL, shiftR)
import Data.ByteString (ByteString)
import Data.Decimal
import Data.Int (Int8, Int32, Int64)
import Data.List (foldl')
import Data.Map (Map)
import Data.Maybe (fromMaybe, isJust)
import Data.Serialize (get, put)
import Data.Serialize.IEEE754
import Data.Serialize.Get hiding (getBytes, Result)
import Data.Serialize.Put
import Data.Set (Set)
import Data.Text (Text)
import Data.UUID (UUID)
import Data.Word (Word8, Word16)
import Network.Socket (SockAddr(SockAddrInet, SockAddrInet6))
import Numeric (showHex)
import Prelude hiding (putStr)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as L
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Text.Encoding as E
import qualified Data.UUID as UUID

-- Encoding / Decoding of the frame header & protocol primitives
-------------------------------------------------------------------------------
type OpCode = Word8

data Header = Header
    { hdrVersion :: Word8
    , hdrFlags :: Word8
    , hdrStreamId :: Int8
    , hdrOpCode :: OpCode
    , hdrLength :: Int32
    } deriving (Show)

getHeader :: Get Header
getHeader = Header
         <$> getWord8
         <*> getWord8
         <*> (fromIntegral <$> getWord8)
         <*> getWord8
         <*> getInt

putHeader :: Header -> Put
putHeader hdr = do
    put (hdrVersion hdr)
    put (hdrFlags hdr)
    put (hdrStreamId hdr)
    put (hdrOpCode hdr)
    put (hdrLength hdr)

putInt :: Integral n => n -> Put
putInt = putWord32be . fromIntegral

getInt :: Get Int32
getInt = get

putShort :: Integral n => n -> Put
putShort = putWord16be . fromIntegral

getShort :: Get Word16
getShort = get

putUUID :: UUID -> Put
putUUID = putLazyByteString . UUID.toByteString

getUUID :: Get UUID
getUUID = uuid >>= maybe (fail "Invalid UUID") return
  where uuid = (UUID.fromByteString . L.fromStrict) <$> getByteString 16

putStr :: Text -> Put
putStr s = do
    putShort $ BS.length bs
    putByteString bs
  where
    bs = E.encodeUtf8 s

getStr :: Get Text
getStr = do
    n  <- getShort
    bs <- getByteString (fromIntegral n)
    return $ E.decodeUtf8 bs

putLongStr :: Text -> Put
putLongStr t = do
    putInt $ T.length t
    putByteString $ E.encodeUtf8 t

getStrList :: Get [Text]
getStrList = do
    n <- getShort
    replicateM (fromIntegral n) getStr

putBytes :: Maybe ByteString -> Put
putBytes maybeBs = case maybeBs of
    Just bs -> do
        putInt $ BS.length bs
        putByteString bs
    Nothing -> putInt (-1 :: Int32)

getBytes :: Get (Maybe ByteString)
getBytes = do
    n <- fromIntegral <$> getInt
    if n >= 0
        then Just <$> getByteString n
        else return Nothing

putShortBytes :: ByteString -> Put
putShortBytes bs = do
    putShort $ BS.length bs
    putByteString bs

getShortBytes :: Get ByteString
getShortBytes = getShort >>= getByteString . fromIntegral

putStrMap :: [(Text, Text)] -> Put
putStrMap m = do
    putShort $ length m
    forM_ m $ \(k, v) -> putStr k >> putStr v

getStrMultiMap :: Get [(Text, [Text])]
getStrMultiMap = do
    n <- fromIntegral <$> getShort
    replicateM n ((,) <$> getStr <*> getStrList)

putShortLen :: [a] -> Put
putShortLen = putShort . length

putConsistency :: Consistency -> Put
putConsistency c = putWord16be $ case c of
    ANY          -> 0x0000
    ONE          -> 0x0001
    TWO          -> 0x0002
    THREE        -> 0x0003
    QUORUM       -> 0x0004
    ALL          -> 0x0005
    LOCAL_QUOROM -> 0x0006
    EACH_QUORUM  -> 0x0007

getConsistency :: Get Consistency
getConsistency = do
    code <- getWord16be
    case code of
        0x0000 -> return ANY
        0x0001 -> return ONE
        0x0002 -> return TWO
        0x0003 -> return THREE
        0x0004 -> return QUORUM
        0x0005 -> return ALL
        0x0006 -> return LOCAL_QUOROM
        0x0007 -> return EACH_QUORUM
        _      -> fail $ "Unrecognized consistency level: " ++ show code

putRequestMessage :: RequestMessage -> (OpCode, Put)
putRequestMessage msg = case msg of
    Startup v compr   -> (0x01, startupBody v compr)
    Credentials creds -> (0x04, putStrMap creds)
    Options           -> (0x05, return ())
    Query q c         -> (0x07, queryBody q c)
    Prepare q         -> (0x09, putLongStr q)
    Execute (PreparedQueryId qid) vs c -> (0x0A, execBody qid vs c)
    Register evTypes  -> (0x0B, registerBody evTypes)
  where
    startupBody v compr = putStrMap $ ver v ++ maybe [] alg compr
                            where
                              alg Snappy = [("COMPRESSION", "snappy")]
                              alg Zlib   = [("COMPRESSION", "zlib")]
                              ver CQLv3  = [("CQL_VERSION", "3.0.0")]
    queryBody q c       = putLongStr q >> putConsistency c
    execBody qid vs c   = do putShortBytes qid
                             putShortLen vs
                             mapM_ putBytes vs
                             putConsistency c
    registerBody ets    = do putShortLen ets
                             mapM_ putEventType ets

getResponse :: Header -> Get Response
getResponse hdr = do
    traceId  <- if (hdrFlags hdr) .&. 0x02 == 0x02
                    then Just <$> getUUID
                    else return Nothing
    msg      <- getResponseMessage hdr
    return $ Response msg (hdrStreamId hdr) traceId

getResponseMessage :: Header -> Get ResponseMessage
getResponseMessage hdr = case (hdrOpCode hdr) of
    0x00 -> getError
    0x02 -> return Ready
    0x03 -> Authenticate <$> getStr
    0x06 -> getSupported
    0x08 -> Result <$> result
    0x0C -> Event <$> getEvent
    _    -> fail $ "Unrecognized response opcode: " ++ (show $ hdrOpCode hdr)
  where
    result = getInt >>= (\kind ->
        case kind of
            0x0001 -> return Void
            0x0002 -> getRows
            0x0003 -> SetKeyspace <$> getStr
            0x0004 -> Prepared <$> PreparedQueryId <$> getShortBytes
            0x0005 -> SchemaChange <$> getSchemaChangeType <*> getStr <*> getStr
            _      -> fail $ "Unrecognized result type: " ++ show kind)

getSupported :: Get ResponseMessage
getSupported = do
    opts <- getStrMultiMap
    (versions, compressions) <- foldM parseOpts ([], []) opts
    return $ Supported versions compressions
  where
    parseOpts :: ([CQLVersion], [Compression]) -> (Text, [Text]) -> Get ([CQLVersion], [Compression])
    parseOpts (vs, cs) (k, v) = case k of
        "CQL_VERSION" -> (,) <$> foldM parseVersion [] v <*> return cs
        "COMPRESSION" -> (,) <$> return vs <*> foldM parseCompression [] v
        _ -> fail $ "Unrecognized option: " ++ show k

    parseVersion :: [CQLVersion] -> Text -> Get [CQLVersion]
    parseVersion vs v = case T.uncons v of
        Just ('3', _) -> return $! CQLv3 : vs
        _ -> fail $ "Unrecognized version: " ++ show v

    parseCompression :: [Compression] -> Text -> Get [Compression]
    parseCompression cs c = case c of
        "snappy" -> return $! Snappy : cs
        "zlib" -> return $! Zlib : cs
        _ -> fail $ "Unrecognized compression: " ++ show c

getError :: Get ResponseMessage
getError = do
    code <- getInt
    msg  <- getStr
    Error msg <$> case code of
        0x0000 -> return ServerError
        0x000A -> return ProtocolError
        0x0100 -> return BadCredentials
        0x1000 -> Unavailable <$> getConsistency <*> getInt <*> getInt
        0x1001 -> return Overloaded
        0x1002 -> return IsBootstrapping
        0x1003 -> return TruncateError
        0x1100 -> WriteTimeout <$> getConsistency <*> getInt <*> getInt <*> getWriteType
        0x1200 -> ReadTimeout <$> getConsistency <*> getInt <*> getInt <*> getBool
        0x2000 -> return SyntaxError
        0x2100 -> return Unauthorized
        0x2200 -> return Invalid
        0x2300 -> return ConfigError
        0x2400 -> AlreadyExists <$> getStr <*> getStr
        0x2500 -> Unprepared <$> getShortBytes
        _      -> fail $ "Unrecognized error code: 0x" ++ (showHex code "")

getWriteType :: Get WriteType
getWriteType = do
    name <- getStr
    case name of
        "SIMPLE"         -> return SIMPLE
        "BATCH"          -> return BATCH
        "UNLOGGED_BATCH" -> return UNLOGGED_BATCH
        "COUNTER"        -> return COUNTER
        "BATCH_LOG"      -> return BATCH_LOG
        _                -> fail $ "Unrecognized write type: " ++ show name

getBool :: Get Bool
getBool = (/=0) <$> getWord8

getRows :: Get Result
getRows = do
    flags        <- getInt
    let gobalTbl  = flags .&. 0x0001 == 0x0001
    colCount     <- fromIntegral <$> getInt
    globalTable  <- if gobalTbl
                        then Just <$> tableSpec
                        else return Nothing
    cols         <- replicateM colCount (getColInfo globalTable)
    rowCount     <- fromIntegral <$> getInt
    rows         <- replicateM rowCount (replicateM colCount getBytes)
    return $ Rows cols rows
  where
    tableSpec = (,) <$> getStr <*> getStr
    getColInfo t = do
        (ksname, table) <- maybe tableSpec return t
        cname <- getStr
        ctype <- getColType
        return $ ColumnInfo ksname table cname ctype
    getColType = do
        typeId <- getShort
        case typeId of
            0x0000 -> CCustom <$> getStr
            0x0001 -> return CAscii
            0x0002 -> return CBigint
            0x0003 -> return CBlob
            0x0004 -> return CBoolean
            0x0005 -> return CCounter
            0x0006 -> return CDecimal
            0x0007 -> return CDouble
            0x0008 -> return CFloat
            0x0009 -> return CInt
            0x000A -> return CText
            0x000B -> return CTimestamp
            0x000C -> return CUUID
            0x000D -> return CVarchar
            0x000E -> return CVarint
            0x000F -> return CTimeUUID
            0x0010 -> return CInet
            0x0020 -> CList <$> getColType
            0x0021 -> CMap  <$> getColType <*> getColType
            0x0022 -> CSet  <$> getColType
            _      -> fail $ "Unrecognized type ID: " ++ show typeId

getEvent :: Get Event
getEvent = getStr >>= (\s ->
    case s of
        "TOPOLOGY_CHANGE" -> TopologyChanged <$> getTopologyChangeType <*> getSockAddr
        "STATUS_CHANGE"   -> StatusChanged <$> getStatusChangeType <*> getSockAddr
        "SCHEMA_CHANGE"   -> SchemaChanged <$> getSchemaChangeType <*> getStr <*> getStr
        _                 -> fail $ "Unrecognized event: " ++ show s)

putEventType :: EventType -> Put
putEventType et = case et of
    TOPOLOGY_CHANGE -> putStr "TOPOLOGY_CHANGE"
    STATUS_CHANGE -> putStr "STATUS_CHANGE"
    SCHEMA_CHANGE -> putStr "SCHEMA_CHANGE"

-- SockAddr representation as in 'Event' responses.
-- Note that this is different from the SockAddr representation
-- of values (FromCasValue / ToCasValue instances).
getSockAddr :: Get SockAddr
getSockAddr = do
    n <- getWord8
    case n of
        4 -> do
            ipv4 <- getIPv4
            port <- getPort
            return $ SockAddrInet port ipv4
        16 -> do
            ipv6 <- getIPv6
            port <- getPort
            return $ SockAddrInet6 port 0 ipv6 0
        _  -> fail $ "Unexpected length of inet address: " ++ show n
  where
    getPort = fromIntegral <$> getInt
    getIPv4 = getWord32be
    getIPv6 = (,,,) <$> getWord32be <*> getWord32be <*> getWord32be <*> getWord32be

getTopologyChangeType :: Get TopologyChangeType
getTopologyChangeType = do
    s <- getStr
    case s of
        "NEW_NODE" -> return NEW_NODE
        "REMOVED_NODE" -> return REMOVED_NODE
        _ -> fail $ "Unrecognized topology event: " ++ show s

getStatusChangeType :: Get StatusChangeType
getStatusChangeType = do
    s <- getStr
    case s of
        "UP" -> return UP
        "DOWN" -> return DOWN
        _ -> fail $ "Unrecognized node status event: " ++ show s

getSchemaChangeType :: Get SchemaChangeType
getSchemaChangeType = do
    s <- getStr
    case s of
        "CREATED" -> return CREATED
        "UPDATED" -> return UPDATED
        "DROPPED" -> return DROPPED
        _ -> fail $ "Unrecognized schema change event: " ++ show s


-- Request Construction
-------------------------------------------------------------------------------
-- | Constructs a simple 'Request' with the given message, a 'reqStreamId' of 0,
-- no compression and no tracing.
simpleRequest :: RequestMessage -> Request
simpleRequest msg = Request msg 0 False

-- | A simple startup request with 'CQLv3' and no compression.
startupRequest :: Request
startupRequest = simpleRequest $ Startup CQLv3 Nothing

-- | Constructs a simple 'Query' request with a stream ID of 0,
-- no compression and no tracing.
simpleQuery :: Text -> Consistency -> Request
simpleQuery q = simpleRequest . (Query q)


-- Request / Response Encoding / Decoding
-------------------------------------------------------------------------------

-- | Encode the given request into a 'ByteString', representing a frame of
-- the protocol that can be sent to a Cassandra server.
encodeRequest :: Request -> Maybe Compress -> ByteString
encodeRequest req compr = runPut $ do
    putHeader $ mkHeader (fromIntegral $ BS.length bs) opc
    putByteString bs
  where
    (opc, enc) = putRequestMessage (reqMessage req)
    bs = compress $ runPut enc
    compress b = case compr of
        Just f  -> fromMaybe b (f b)
        Nothing -> b
    mkHeader len opcode = Header
        { hdrVersion = 0x01
        , hdrFlags = comprFlag .|. traceFlag
        , hdrStreamId = reqStreamId req
        , hdrOpCode = opcode
        , hdrLength = len
        }
    comprFlag = if isJust compr then 1 else 0
    traceFlag = if reqTracing req then 2 else 0

data DecodeFailure
    = DecodeHeaderFailure String
    | DecompressionFailure
    | DecodeBodyFailure String

instance Show DecodeFailure where
    show (DecodeHeaderFailure msg) = "Failed to decode frame header: " ++ msg
    show DecompressionFailure = "Unable to decompress response body."
    show (DecodeBodyFailure msg) = "Failed to decode frame body: " ++ msg

-- | Decode a response in the context of an arbitrary monad m.
decodeResponse :: Monad m =>
       (Int -> m ByteString)
        -- ^ Provides the 'ByteString's to decode.
    -> Maybe Decompress
        -- ^ The function to use for decompressing the response, if necessary.
    -> m (Either DecodeFailure Response)
decodeResponse recv compr = do
    hbs <- recv 8
    case runGet getHeader hbs of
        Left e -> return (Left $ DecodeHeaderFailure e)
        Right hdr -> do
            bbs <- recv (fromIntegral $ hdrLength hdr)
            return $ decompress bbs >>= decode
          where
            decompress bs =
                if (hdrFlags hdr) .&. 0x01 == 0x01
                    then maybe (Left DecompressionFailure) Right (compr >>= \f -> f bs)
                    else Right bs
            decode = (left DecodeBodyFailure) . runGet (getResponse hdr)

-- $compression
-- To use compression, specify one of the supported 'Compression' options in
-- the 'Startup' message on the connection on which you wish to use compression.
-- Then pass the appropriate 'Compress'ion and 'Decompress'ion functions to
-- 'encodeRequest' and 'decodeResponse', respectively. You may want to ask
-- the server for its supported compression algorithms by sending an 'Options'
-- message before the 'Startup', which will return the 'Supported' options.
--
-- Example using the /snappy/ module:
--
-- @
--     import qualified Codec.Compression.Snappy as Snappy
--     ...
--     decodeResponse (hGet h) (Just $ Just . Snappy.decompress)
-- @
--
-- Example using the /lz4/ module:
--
-- @
--     import qualified Codec.Compression.LZ4 as LZ4
--     ...
--     decodeResponse (hGet h) (Just LZ4.decompress)
-- @

-- | The type of compression function used in 'encodeRequest'.
-- If the function returns 'Nothing', e.g. if it fails (non-fatally) or if you only
-- want to encode requests above a certain size, the request will remain uncompressed.
type Compress = ByteString -> Maybe ByteString

-- | The type of decompression function used in 'decodeResponse'.
-- If the function returns 'Nothing', decoding will fail.
type Decompress = ByteString -> Maybe ByteString


-- Encoding & Decoding of Values (parameters and result rows)
-------------------------------------------------------------------------------

newtype Blob = Blob ByteString deriving (Eq, Ord, Show, ToCasValue, FromCasValue)
newtype Counter = Counter Int64 deriving (Eq, Ord, Show, Read, ToCasValue, FromCasValue)
newtype Timestamp = Timestamp Int64 deriving (Eq, Ord, Show, Read, ToCasValue, FromCasValue)
newtype TimeUUID = TimeUUID UUID deriving (Eq, Ord, Read, Show, ToCasValue, FromCasValue)

-- | Container useful for building up a heterogenous list of types
-- (usually query parameters) that can be encoded via 'encodeValues'.
data Param = forall a. ToCasValue a => Param a

-- | Class of types that can be serialized to a single Cassandra value.
class ToCasValue a where
    -- | Encodes a value into Cassandra binary format.
    -- Returning 'Nothing' corresponds to a Cassandra NULL value,
    -- for which no binary value exists as it is identified
    -- by a -1 length indicator preceeding the value.
    putCasValue :: a -> Maybe Put

-- | Class of types that can be parsed from a single Cassandra value.
class FromCasValue a where
    -- | Decodes a (non-null) value from its Cassandra binary format.
    getCasValue :: Get a
    -- | The value to stand in for Cassandra NULL values on decoding, if any.
    fromCasNull :: Maybe a
    fromCasNull = Nothing
    -- casType :: CType -- TODO: For better error messages in 'decodeValue'?
    -- hsType :: String -- TODO: For better error messages in 'decodeValue'?

-- | Encode a single value into the Cassandra binary format.
encodeValue :: ToCasValue a => a -> Maybe ByteString
encodeValue a = runPut <$> putCasValue a

-- | Decode a single value from the Cassandra binary format.
decodeValue :: FromCasValue a => Maybe ByteString -> Either String a
decodeValue mbs = case mbs of
    Just bs -> runGet getCasValue bs
    Nothing -> maybe (Left "Missing value") Right fromCasNull

instance ToCasValue a => ToCasValue (Maybe a) where
    putCasValue Nothing = Nothing
    putCasValue (Just a) = putCasValue a

instance FromCasValue a => FromCasValue (Maybe a) where
    getCasValue = Just <$> getCasValue
    fromCasNull = Just Nothing

instance ToCasValue ByteString where
    putCasValue = Just . putByteString

instance FromCasValue ByteString where
    getCasValue = getByteString =<< remaining

instance ToCasValue Int32 where
    putCasValue = Just . putWord32be . fromIntegral

instance FromCasValue Int32 where
    getCasValue = fromIntegral <$> getWord32be

instance ToCasValue Int64 where
    putCasValue = Just . putWord64be . fromIntegral

instance FromCasValue Int64 where
    getCasValue = fromIntegral <$> getWord64be

instance ToCasValue Text where
    putCasValue = Just . putByteString . E.encodeUtf8

instance FromCasValue Text where
    getCasValue = E.decodeUtf8 <$> (getByteString =<< remaining)

putCollElem :: ToCasValue a => a -> Put
putCollElem a = case (encodeValue a) of
    Just bs -> putShort (BS.length bs) >> putByteString bs
    Nothing -> error "Null values are not allowed in collections"

instance ToCasValue a => ToCasValue [a] where
    putCasValue as = Just $ do
        putShortLen as
        forM_ as putCollElem

instance FromCasValue a => FromCasValue [a] where
    getCasValue = do
        n <- getShort
        replicateM (fromIntegral n) $ do
            len <- getShort
            -- no check for len <= 0 as null values are not allowed in collections
            bs  <- Just <$> getByteString (fromIntegral len)
            either fail return (decodeValue bs)

instance (ToCasValue a, Ord a, ToCasValue b) => ToCasValue (Map a b) where
    putCasValue m = Just $ do
        let elems = Map.toList m
        putShortLen elems
        forM_ elems $ \(a, b) -> putCollElem a >> putCollElem b

instance (FromCasValue a, Ord a, FromCasValue b) => FromCasValue (Map a b) where
    -- TODO: Decode directly into a Map, not via Map.fromList
    getCasValue = do
        n <- getShort
        elems <- replicateM (fromIntegral n) $ do
            kbs <- getShort >>= getByteString . fromIntegral
            k   <- either fail return $ decodeValue (Just kbs)
            vbs <- getShort >>= getByteString . fromIntegral
            v   <- either fail return $ decodeValue (Just vbs)
            return (k, v)
        return $ Map.fromList elems

instance (ToCasValue a, Ord a) => ToCasValue (Set a) where
    putCasValue = putCasValue . Set.toList

instance (FromCasValue a, Ord a) => FromCasValue (Set a) where
    getCasValue = Set.fromList <$> getCasValue

instance ToCasValue UUID where
    putCasValue = Just <$> putUUID

instance FromCasValue UUID where
    getCasValue = getUUID

instance ToCasValue Bool where
    putCasValue True = Just (putWord8 1)
    putCasValue False = Just (putWord8 0)

instance FromCasValue Bool where
    getCasValue = getBool

instance ToCasValue Integer where
    putCasValue i = Just $ putByteString . BS.pack $
        if i < 0
            then encodeNeg $ positivize 0x80 i
            else encodePos i
      where
        encodePos :: Integer -> [Word8]
        encodePos = reverse . enc
          where
            enc j | j == 0   = [0]
            enc j | j < 0x80 = [fromIntegral j]
            enc j            = fromIntegral j : enc (j `shiftR` 8)
        encodeNeg :: Integer -> [Word8]
        encodeNeg = reverse . enc
          where
            enc j | j == 0    = []
            enc j | j < 0x100 = [fromIntegral j]
            enc j             = fromIntegral j : enc (j `shiftR` 8)
        positivize :: Integer -> Integer -> Integer
        positivize bits j = case bits + j of
                                j' | j' >= 0 -> j' + bits
                                _            -> positivize (bits `shiftL` 8) j

instance FromCasValue Integer where
    getCasValue = do
        ws <- BS.unpack <$> (getByteString =<< remaining)
        return $
            if null ws
                then 0
                else
                    let i = foldl' (\j w -> j `shiftL` 8 + fromIntegral w) 0 ws
                    in  if head ws >= 0x80
                            then i - 1 `shiftL` (length ws * 8)
                            else i

instance ToCasValue Decimal where
    putCasValue (Decimal places mantissa) = Just $ do
        putWord32be (fromIntegral places)
        -- safe as per the Integer instance (Decimal = DecimalRaw Integer)
        fromMaybe (error "Illegal null in decimal mantissa") (putCasValue mantissa)

instance FromCasValue Decimal where
    getCasValue = Decimal <$> (fromIntegral . min 0xff <$> getWord32be) <*> getCasValue

instance ToCasValue Double where
    putCasValue = Just . putFloat64be

instance FromCasValue Double where
    getCasValue = getFloat64be

instance ToCasValue Float where
    putCasValue = Just . putFloat32be

instance FromCasValue Float where
    getCasValue = getFloat32be

instance FromCasValue SockAddr where
    getCasValue = do
        len <- remaining
        case len of
            4 -> SockAddrInet 0 <$> getIPv4
            16 -> SockAddrInet6 0 0 <$> getIPv6 <*> return 0
            _  -> fail $ "Unexpected length of inet address: " ++ show len
      where
        getIPv4 = getWord32be
        getIPv6 = (,,,) <$> getWord32be <*> getWord32be <*> getWord32be <*> getWord32be

instance ToCasValue SockAddr where
    putCasValue sa = Just $ do
        case sa of
            SockAddrInet _ w -> putWord32be w
            SockAddrInet6 _ _ (a,b,c,d) _ -> do
                putWord32be a
                putWord32be b
                putWord32be c
                putWord32be d
            _ -> fail $ "Unsupported SockAddr: " ++ show sa


-- | Class of types that can be serialized to a list of Cassandra values
-- (e.g. that can be passed to 'Execute').
class ToCasValues a where
    encodeValues :: a -> [Maybe ByteString]

-- | Class of types that can be parsed from a list of a Cassandra values
-- (e.g. a query result row as returned by the 'Rows' result).
class FromCasValues a where
    decodeValues :: [Maybe ByteString] -> Either String a

decodeRows :: FromCasValues a => [[Maybe ByteString]] -> Either String [a]
decodeRows = mapM decodeValues

decodeValuesN :: (FromCasValue a, FromCasValues b) => [Maybe ByteString] -> Either String (a, b)
decodeValuesN [] = Left "Missing at least one value to decode row."
decodeValuesN (bs:bss) = (,) <$> decodeValue bs <*> decodeValues bss

instance ToCasValue a => ToCasValues [a] where
    encodeValues = map (fmap runPut . putCasValue)

instance FromCasValue a => FromCasValues [a] where
    decodeValues = mapM decodeValue

instance (ToCasValue a, ToCasValue b) => ToCasValues (a, b) where
    encodeValues (a,b) = [encodeValue a, encodeValue b]

instance (FromCasValue a, FromCasValue b) => FromCasValues (a, b) where
    decodeValues [] = Left "Missing at least two values to decode row."
    decodeValues (_:[]) = Left "Missing at least one value to decode row."
    decodeValues (bs1:bs2:_) = (,) <$> decodeValue bs1 <*> decodeValue bs2

instance (ToCasValue a, ToCasValue b, ToCasValue c) => ToCasValues (a, b, c) where
    encodeValues (a,b,c) = encodeValue a : encodeValues (b, c)

instance (FromCasValue a, FromCasValue b, FromCasValue c) => FromCasValues (a, b, c) where
    decodeValues bss = (\(a, (b, c)) -> (a, b, c)) <$> decodeValuesN bss

instance (ToCasValue a, ToCasValue b, ToCasValue c, ToCasValue d) => ToCasValues (a, b, c, d) where
    encodeValues (a,b,c,d) = encodeValue a : encodeValues (b, c, d)

instance (FromCasValue a, FromCasValue b, FromCasValue c, FromCasValue d) => FromCasValues (a, b, c, d) where
    decodeValues bss = (\(a, (b, c, d)) -> (a, b, c, d)) <$> decodeValuesN bss

instance (ToCasValue a, ToCasValue b, ToCasValue c, ToCasValue d,
          ToCasValue e) => ToCasValues (a, b, c, d, e) where
    encodeValues (a,b,c,d,e) = encodeValue a : encodeValues (b, c, d, e)

instance (FromCasValue a, FromCasValue b, FromCasValue c, FromCasValue d,
          FromCasValue e) => FromCasValues (a, b, c, d, e) where
    decodeValues bss = (\(a, (b, c, d, e)) -> (a, b, c, d, e)) <$> decodeValuesN bss

instance (ToCasValue a, ToCasValue b, ToCasValue c, ToCasValue d,
          ToCasValue e, ToCasValue f) => ToCasValues (a, b, c, d, e, f) where
    encodeValues (a,b,c,d,e,f) = encodeValue a : encodeValues (b, c, d, e, f)

instance (FromCasValue a, FromCasValue b, FromCasValue c, FromCasValue d,
          FromCasValue e, FromCasValue f) => FromCasValues (a, b, c, d, e, f) where
    decodeValues bss = (\(a, (b, c, d, e, f)) -> (a, b, c, d, e, f)) <$> decodeValuesN bss

-- TODO: More tuple instances
