{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.Types
  ( NodeId (..)
  , nodeId
  , nodeIdSize
  , NodeMap
  , SeqNum
  , PostCount
  , SendTarget (..)
  , TransmissionMode (..)
  , CacheMode (..)
  ) where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Data.ByteString (ByteString)
import Data.IP (IP)
import Data.Map (Map)
import Data.Set (Set)
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

data SendTarget
  = SendTo (Set NodeId)
  | SendAll
  deriving (Show,Eq,Ord)

data TransmissionMode
  = TMLossy Double
    -- ^ Fail to send messages according to the given probability
    -- (between @0@ and @1@).
  | TMNormal
    -- ^ Attempt to send all messages.
  | TMSubNetwork SendTarget
    -- ^ Only attempt to send messages to peers designated by the
    -- given 'SendTarget', simulating broken links to the other peers.

data CacheMode
  = CacheNone -- ^ Do not cache messages, disabling retransmission
  | CacheTemp -- ^ Cache all messages until they are universally delivered
  | CacheForever -- ^ Cache all messages forever
  deriving (Show,Eq,Ord)
