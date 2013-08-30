{-# LANGUAGE OverloadedStrings #-}
module Tests.Database.Cassandra.CQL.Protocol (tests) where

import Control.Applicative ((<$>), (<*>))
import Control.Exception.Lifted (finally)
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Reader
import Data.ByteString (ByteString, hPut, hGet)
import Data.Decimal
import Data.Int (Int32, Int64)
import Data.Map (Map)
import Data.Maybe (isJust)
import Data.Set (Set)
import Data.Text (Text)
import Data.UUID (UUID)
import Database.Cassandra.CQL.Protocol
import Network (connectTo, PortID(PortNumber))
import Network.Socket (SockAddr(SockAddrInet))
import System.IO (Handle, hClose, hSetBuffering, BufferMode(NoBuffering))
import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)

-- TODO: Test zlib compression, requires at least Cassandra 2.0.0-beta2
-- import qualified Codec.Compression.LZ4 as LZ4
import qualified Codec.Compression.Snappy as Snappy
import qualified Data.ByteString.Char8 as C
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.UUID as UUID

import Data.Bits (shiftL)

-- TODO: Share connection between (some) tests (compression is per connection)?
tests :: [Test]
tests = [ testStartup
        , testOptions
        , testRowsQuery
        , testQueryWithTracing
        , testPrepareExecute
        , testRegisterEvent
        , testSnappyCompression
        ]

testStartup :: Test
testStartup = testCase "Startup -> Ready" $ withConnection $ sendRequest req >>= check
  where
    req = startupRequest
    check (Response Ready 0 Nothing) = return ()
    check r = failure r

testOptions :: Test
testOptions = testCase "Options -> Supported" $ withConnection $ sendRequest req >>= check
  where
    req = Request Options 0 False
    check (Response (Supported _ _) 0 Nothing) = return ()
    check r = failure r

testRowsQuery :: Test
testRowsQuery = testCase "Query -> Result (Rows)" $ withConnection $ withTestSchema $ do
    query_ "INSERT INTO test (id, vset) VALUES (1, {'ac@d.c'})"
    query_ "INSERT INTO test (id, vset) VALUES (2, {'joe@acme.com'})"
    query "SELECT id, vset FROM test WHERE id = 2;" >>= liftIO . check
  where
    check (Response (Result (Rows meta rows)) _ _) = do
        let expectedMeta =
                [ ColumnInfo "prototest" "test" "id" CInt
                , ColumnInfo "prototest" "test" "vset" (CSet CVarchar)
                ]
            expectedRows = Right [(2 :: Int32, Set.singleton "joe@acme.com" :: Set Text)]
            actualRows = decodeRows rows
        assertEqual "rows.meta" expectedMeta meta
        assertEqual "rows" expectedRows actualRows
    check x = failure x

testQueryWithTracing :: Test
testQueryWithTracing = testCase "Query with tracing" $ withConnection $ withTestSchema $ do
    query_ "INSERT INTO test (id, vset) VALUES (1, {'ac@d.c'})"
    sendRequest req >>= check
  where
    req = Request q 0 True -- with tracing
    q = Query "SELECT id FROM test WHERE id=1;" QUORUM
    check r = liftIO $ assertBool "Trace ID must be set" $ isJust (rspTraceId r)

testPrepareExecute :: Test
testPrepareExecute = testCase "Prepare -> Execute -> Result (Rows)" $ withConnection $ withTestSchema $ do
    qid <- sendRequest prepReq >>= checkPrep
    sendRequest (execReq qid testRecord1) >>= checkExec
    sendRequest (execReq qid testRecord2) >>= checkExec
    query testRecordSelectSQL >>= checkQry
  where
    prepReq = simpleRequest $ Prepare testRecordInsertCQL
    checkPrep (Response (Result (Prepared qid)) 0 Nothing) = return qid
    checkPrep r = lift $ MaybeT $ failure r >> return Nothing

    execReq qid r = simpleRequest $ Execute qid (encodeValues r) QUORUM
    checkExec (Response (Result Void) 0 Nothing) = return ()
    checkExec r = failure r

    checkQry r = liftIO $ case (rspMessage r) of
        Result (Rows _ rows) -> assertEqual "prepare.execute.rows"
                                    (Right [testRecord1, testRecord2])
                                    (decodeRows rows)
        x -> failure x

testRegisterEvent :: Test
testRegisterEvent = testCase "Register -> Event" $ withConnection $ withTestSchema $ do
    sendRequest_ $ Request (Register [SCHEMA_CHANGE]) 0 False
    query_ "CREATE TABLE test2 (id INT PRIMARY KEY)"
    h <- ask
    rsp <- liftIO $ decodeResponse (hGet h) Nothing
    liftIO $ case rsp of
        Left e -> assertFailure (show e)
        Right (Response msg _ _) -> check msg
  where
    check (Result (SchemaChange CREATED "prototest" "test2")) = return ()
    check r = failure r

testSnappyCompression :: Test
testSnappyCompression = testCase "Snappy Compression" $ withConnection $ do
    r1 <- sendRequest $ Request Options 0 False
    case (rspMessage r1) of
        Supported _ cs | Snappy `elem` cs -> do
            h <- ask
            let req = Request (Startup CQLv3 (Just Snappy)) 0 False
            liftIO $ do
                hPut h (encodeRequest req Nothing)
                rsp <- decodeResponse (hGet h) (Just $ Just . Snappy.decompress)
                check rsp
        Supported _ _ -> liftIO $ putStrLn $ "Snappy not supported, ignoring test."
        _ -> failure r1
  where
    check (Right (Response Ready 0 Nothing)) = return ()
    check r = failure r

-- HELPERS
-------------------------------------------------------------------------------

type ProtoAction a = ReaderT Handle (MaybeT IO) a

withConnection :: ProtoAction a -> Assertion
withConnection action = do
    h <- connectTo "localhost" (PortNumber 9042)
    hSetBuffering h NoBuffering
    (runMaybeT (runReaderT action h) >> return ()) `finally` hClose h

sendRequest :: Request -> ProtoAction Response
sendRequest req = do
    h <- ask
    lift $ MaybeT $ do
        hPut h (encodeRequest req Nothing)
        rsp <- decodeResponse (hGet h) Nothing
        case rsp of
            Left e -> assertFailure (show e) >> return Nothing
            Right r -> return (Just r)

sendRequest_ :: Request -> ProtoAction ()
sendRequest_ req = sendRequest req >>= \r ->
    case (rspMessage r) of
        Error msg detail -> do
            liftIO $ assertFailure $ show msg ++ ": " ++ show detail
            return ()
        _ -> return ()

query :: Text -> ProtoAction Response
query q = sendRequest $ simpleRequest $ Query q QUORUM

query_ :: Text -> ProtoAction ()
query_ q = sendRequest_ $ simpleRequest $ Query q QUORUM

startup_ :: ProtoAction ()
startup_ = sendRequest_ startupRequest

failure :: (Show a, MonadIO m) => a -> m ()
failure a = liftIO $ assertFailure $ "Unexpected result: " ++ show a

-- TODO: Randomize keyspace name to avoid accidental collisions
withTestSchema :: ProtoAction a -> ProtoAction a
withTestSchema action = do
    startup_
    query_ createKeyspace
    query_ useKeyspace
    query_ createTypeTable
    action
  `finally`
    query_ dropKeyspace
  where
    createKeyspace =
        "CREATE KEYSPACE prototest WITH replication = \
            \{ 'class' : 'SimpleStrategy', 'replication_factor' : '1' };"
    useKeyspace = "USE prototest;"
    createTypeTable =
        "CREATE TABLE test (\
            \id INT PRIMARY KEY,\
            \vascii ASCII,\
            \vbigint BIGINT,\
            \vbool BOOLEAN,\
            \vdecimal DECIMAL,\
            \vdouble DOUBLE,\
            \vfloat FLOAT,\
            \vtext TEXT,\
            \vuuid UUID,\
            \vvarint VARINT,\
            \vinet INET,\
            \vlist LIST<TEXT>,\
            \vset SET<TEXT>,\
            \vmap MAP<TEXT,TEXT>\
        \);"
    dropKeyspace = "DROP KEYSPACE prototest;"

data TestRecord = TestRecord
    { recId :: Int32
    , recAscii :: ByteString
    , recBigint :: Int64
    , recBool :: Bool
    , recDecimal :: Decimal
    , recDouble :: Double
    , recFloat :: Float
    , recText :: Text
    , recUUID :: Maybe UUID
    , recVarint :: Integer
    , recInet :: SockAddr
    , recList :: [Text]
    , recSet :: Set Text
    , recMap :: Map Int32 Text
    } deriving (Eq, Show)

instance ToCasValues TestRecord where
    encodeValues r = [ encodeValue (recId r)
                     , encodeValue (recAscii r)
                     , encodeValue (recBigint r)
                     , encodeValue (recBool r)
                     , encodeValue (recDecimal r)
                     , encodeValue (recDouble r)
                     , encodeValue (recFloat r)
                     , encodeValue (recText r)
                     , encodeValue (recUUID r)
                     , encodeValue (recVarint r)
                     , encodeValue (recInet r)
                     , encodeValue (recList r)
                     , encodeValue (recSet r)
                     , encodeValue (recMap r)
                     ]

instance FromCasValues TestRecord where
    decodeValues (vId:vAscii:vBigint:vBool:vDec:vDouble:vFloat:vText:vUUID:vVarint:vInet:vList:vSet:vMap:_) =
        TestRecord <$> decodeValue vId
                   <*> decodeValue vAscii
                   <*> decodeValue vBigint
                   <*> decodeValue vBool
                   <*> decodeValue vDec
                   <*> decodeValue vDouble
                   <*> decodeValue vFloat
                   <*> decodeValue vText
                   <*> decodeValue vUUID
                   <*> decodeValue vVarint
                   <*> decodeValue vInet
                   <*> decodeValue vList
                   <*> decodeValue vSet
                   <*> decodeValue vMap
    decodeValues _ = Left "Not enough values to decode TestRecord."

testRecordInsertCQL :: Text
testRecordInsertCQL =
    "INSERT INTO test (\
        \id, vascii, vbigint, vbool,\
        \vdecimal, vdouble, vfloat,\
        \vtext, vuuid, vvarint, vinet,\
        \vlist, vset, vmap\
    \) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

testRecordSelectSQL :: Text
testRecordSelectSQL =
    "SELECT \
        \id, vascii, vbigint, vbool, vdecimal,\
        \vdouble, vfloat, vtext, vuuid, vvarint,\
        \vinet, vlist, vset, vmap \
    \FROM test"

testRecord1 :: TestRecord
testRecord1 = TestRecord
    { recId = 1
    , recAscii = C.pack "lorem ipsum dolor sit amet"
    , recBigint = 10000000000
    , recBool = True
    , recDecimal = read "12345678901234567890.123456789"
    , recDouble = 1.2345678912345
    , recFloat = 1.2345679
    , recText = "Mýrdalsjökull"
    , recUUID = UUID.fromString "550e8400-e29b-41d4-a716-446655440000"
    , recVarint = 12345678901234567890
    , recInet = SockAddrInet 0 $ (127 `shiftL` 24) + 1
    , recList = ["abc","def","äöü"]
    , recSet = Set.fromList ["abc","def","äöü"]
    , recMap = Map.fromList [(1, "abc"),(2, "def"),(3, "äöü")]
    }

testRecord2 :: TestRecord
testRecord2 = TestRecord
    { recId = 2
    , recAscii = C.pack "The quick brown fox jumps over the lazy dog"
    , recBigint = 20000000000
    , recBool = False
    , recDecimal = read "-12345678901234567890.123456789"
    , recDouble = 2.2345678912345
    , recFloat = 2.2345679
    , recText = "Eyjafjallajökull"
    , recUUID = Nothing
    , recVarint = -12345678901234567890
    , recInet = SockAddrInet 0 $ (127 `shiftL` 24) + 1
    , recList = ["abc","def","äöü"]
    , recSet = Set.fromList ["abc","def","äöü"]
    , recMap = Map.fromList [(1, "abc"),(2, "def"),(3, "äöü")]
    }
