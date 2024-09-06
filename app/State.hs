{-# LANGUAGE TemplateHaskell #-}

module State where

import Config

import Control.Monad.DebugLog
import Network.Ccm
import Network.Ccm.Lens

import Control.Monad.State
import Data.Map (Map)
import qualified Data.Map as Map

data ExState
  = ExState
    { _stNextSend :: Int
    , _stReceived :: Map NodeId Int
    , _stConf :: NodeConfig
    }

makeLenses ''ExState

type ExMsg = Int

type ExM = StateT ExState (CcmT (LogIO IO))

exStateInit :: NodeConfig -> ExState
exStateInit c = ExState
  { _stNextSend = 0
  , _stReceived = Map.empty
  , _stConf = c
  }

runExM :: ExM a -> NodeConfig -> LogIO IO a
runExM m c =
  let
    self = c ^. cNodeId
    net = c ^. cExpr . cNetwork
    s = exStateInit c
  in
    runCcm defaultCcmConfig self net (evalStateT m s)
