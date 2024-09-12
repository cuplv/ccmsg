{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Config where

import Network.Ccm
import Network.Ccm.Lens

import Data.Map (Map)
import qualified Data.Map as Map
import qualified Dhall
import Data.Text (pack)

data ExConfig
  = ExConfig
    { _cNetwork :: Map NodeId MyAddr
    , _cMsgCount :: Int
    , _cSetupTimeout :: Maybe Int
    , _cRecvTimeout :: Maybe Int
    , _cSendChance :: Maybe Double
    , _cMissingLinks :: Bool
    }

makeLenses ''ExConfig

data NodeConfig
  = NodeConfig
    { _cExpr :: ExConfig
    , _cNodeId :: NodeId
    , _cDebugLog :: [String]
    }

makeLenses ''NodeConfig

nodeIdD :: Dhall.Decoder NodeId
nodeIdD = nodeId.fromIntegral <$> Dhall.natural

addrD :: Dhall.Decoder (NodeId, MyAddr)
addrD =
  let f (i,h,p) = (nodeId (fromIntegral i), (MyAddr h p))
      d = Dhall.record $ (,,)
            <$> Dhall.field "id" Dhall.natural
            <*> Dhall.field "host" Dhall.string
            <*> Dhall.field "port" Dhall.string
  in f <$> d

exConfigD :: Dhall.Decoder ExConfig
exConfigD = Dhall.record $ ExConfig
  <$> Dhall.field "network" (Map.fromList <$> Dhall.list addrD)
  <*> Dhall.field "msgCount" (fromIntegral <$> Dhall.natural)
  <*> Dhall.field "setupTimeout" (Dhall.maybe (fromIntegral <$> Dhall.natural))
  <*> Dhall.field "recvTimeout" (Dhall.maybe (fromIntegral <$> Dhall.natural))
  <*> Dhall.field "sendChance" (Dhall.maybe Dhall.double)
  <*> Dhall.field "missingLinks" Dhall.bool

nodeConfigD :: Dhall.Decoder NodeConfig
nodeConfigD = Dhall.record $ NodeConfig
  <$> Dhall.field "experiment" exConfigD
  <*> Dhall.field "nodeId" nodeIdD
  <*> Dhall.field "debugLog" (Dhall.list Dhall.string)

inputConfig :: String -> IO NodeConfig
inputConfig s = Dhall.input nodeConfigD (pack s)
