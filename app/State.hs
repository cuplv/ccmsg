{-# LANGUAGE TemplateHaskell #-}

module State where

import Config

import Control.Monad.State
import Network.Ccm
import Network.Ccm.Lens

import Data.Map (Map)
import qualified Data.Map as Map

data ExState
  = ExState
    { _stNextSend :: Int
    , _stReceived :: Map NodeId Int
    , _stConf :: NodeConfig
    , _stDebugger :: Debugger
    , _stSeenErrors :: Int
    }

makeLenses ''ExState

type ExMsg = (NodeId, Int)

type ExT m = StateT ExState (CcmT m)

exStateInit :: Debugger -> NodeConfig -> ExState
exStateInit d c = ExState
  { _stNextSend = 0
  , _stReceived = Map.empty
  , _stConf = c
  , _stDebugger = d
  , _stSeenErrors = 0
  }

runExT :: (MonadIO m) => ExT m a -> Debugger -> NodeConfig -> m a
runExT m d c =
  let
    self = c ^. cNodeId
    net = c ^. cExpr . cNetwork
    s = exStateInit d c
  in
    runCcm d True self net (evalStateT m s)
