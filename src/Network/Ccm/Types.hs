{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.Types
  ( NodeId (..)
  , nodeId
  , nodeIdSize
  , NodeMap
  , SeqNum
  , PostCount
  ) where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Data.ByteString (ByteString)
import Data.IP (IP)
import Data.Map (Map)
import Data.Store
import Data.Store.TH
import Data.Word (Word32)
import Network.Socket (PortNumber,SockAddr)

type SeqNum = Word32

type PostCount = Word32

data NodeId = NodeId { nodeIdWord :: Word32 } deriving (Eq,Ord)

instance Show NodeId where
  show (NodeId n) = "N(" ++ show n ++ ")"

makeStore ''NodeId

nodeIdSize :: Int
nodeIdSize = case (size :: Size NodeId) of
  VarSize _ -> error "NodeId has var size for Store representation"
  ConstSize i -> i

nodeId = NodeId

type NodeMap = Map NodeId (String,String)
