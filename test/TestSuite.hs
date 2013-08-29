module Main where

import Test.Framework (defaultMain, testGroup)
import qualified Tests.Database.Cassandra.CQL.Protocol as Protocol
import qualified Tests.Database.Cassandra.CQL.Protocol.Properties as Properties

main :: IO ()
main = defaultMain tests
  where
    tests =
        [ testGroup "Tests.Database.Cassandra.CQL.Protocol" Protocol.tests
        , testGroup "Tests.Database.Cassandra.CQL.Protocol.Properties" Properties.tests
        ]
