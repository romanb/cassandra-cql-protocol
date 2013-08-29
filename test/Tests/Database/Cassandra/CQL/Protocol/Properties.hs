{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans  #-}
module Tests.Database.Cassandra.CQL.Protocol.Properties (tests) where

import Database.Cassandra.CQL.Protocol

import Control.Applicative ((<$>), (<*>), pure)
import Data.ByteString (ByteString)
import Data.Decimal
import Data.Int
import Data.Map (Map)
import Data.Set (Set)
import Data.Text (Text)
import Data.UUID (UUID)
import Network.Socket (SockAddr(SockAddrInet, SockAddrInet6))
import Test.Framework
import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck

import qualified Data.ByteString as BS
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.UUID as UUID

tests :: [Test]
tests =
    [ testProperty "Int32" testInt32
    , testProperty "Int64" testInt64
    , testProperty "Double" testDouble
    , testProperty "Float" testFloat
    , testProperty "Text" testText
    , testProperty "ByteString" testByteString
    , testProperty "Bool" testBool
    , testProperty "Blob" testBlob
    , testProperty "Counter" testCounter
    , testProperty "Integer" testInteger
    , testProperty "UUID" testUUID
    , testProperty "Decimal" testDecimal
    , testProperty "SockAddr" testSockAddr
    , testProperty "Maybe" testMaybe
    , testProperty "List" testList
    , testProperty "Set" testSet
    , testProperty "Map" testMap
    ]

-- A sensible property that should hold for all types which have instances
-- for both ToCasValue and FromCasValue.
testConversion :: (ToCasValue a, FromCasValue a, Arbitrary a, Eq a) => a -> Bool
testConversion a = decodeValue (encodeValue a) == Right a

testInt32 :: Int32 -> Bool
testInt32 = testConversion

testInt64 :: Int64 -> Bool
testInt64 = testConversion

testDouble :: Double -> Bool
testDouble = testConversion

testFloat :: Float -> Bool
testFloat = testConversion

testText :: Text -> Bool
testText = testConversion

testByteString :: ByteString -> Bool
testByteString = testConversion

testBool :: Bool -> Bool
testBool = testConversion

testBlob :: Blob -> Bool
testBlob = testConversion

testCounter :: Counter -> Bool
testCounter = testConversion

testUUID :: UUID -> Bool
testUUID = testConversion

testInteger :: Integer -> Bool
testInteger = testConversion

testDecimal :: Decimal -> Bool
testDecimal = testConversion

testSockAddr :: SockAddr -> Bool
testSockAddr = testConversion

testMaybe :: Maybe Text -> Bool
testMaybe = testConversion

testList :: [Text] -> Bool
testList = testConversion

testSet :: Set Text -> Bool
testSet = testConversion

testMap :: Map Int32 Text -> Bool
testMap = testConversion

-- Auxiliary instances

instance Arbitrary Text where
    arbitrary = T.pack <$> arbitrary

instance Arbitrary Blob where
    arbitrary = Blob <$> arbitrary

instance Arbitrary Counter where
    arbitrary = Counter <$> arbitrary

instance Arbitrary ByteString where
    arbitrary = BS.pack <$> arbitrary

instance Arbitrary Decimal where
    arbitrary = Decimal <$> arbitrary <*> arbitrary

instance Arbitrary UUID where
    arbitrary = UUID.fromWords <$> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary

instance Arbitrary SockAddr where
    arbitrary = oneof [genIPv4, genIPv6]
      where
        genIPv4 = SockAddrInet 0 <$> arbitrary
        genIPv6 = SockAddrInet6 0 0 <$> arbitrary <*> pure 0

instance (Arbitrary a, Ord a) => Arbitrary (Set a) where
    arbitrary = Set.fromList `fmap` arbitrary

instance (Arbitrary a, Arbitrary b, Ord a) => Arbitrary (Map a b) where
    arbitrary = Map.fromList `fmap` arbitrary
