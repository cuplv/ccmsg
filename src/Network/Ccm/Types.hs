{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.Types
  ( NodeId (..)
  , nodeId
  , nodeIdSize
  , NodeMap
  , SeqNum
  , SeqNum'
  , infLE
  , infLT
  , snFin
  , snInf
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
    -- ^ Successfully send messages according to the given probability
    -- (between @0@ and @1@).  For example, @'TMLossy' 0.75@ means
    -- that each message has a @3/4@ chance of being successfully
    -- sent, and a @1/4@ chance of being "dropped" (not sent).
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

{- | Possibly-infinite 'SeqNum', where 'Nothing' represents infinity. -}
type SeqNum' = Maybe SeqNum

{- | Compare '<=' for 'SeqNum''s -}
infLE :: SeqNum' -> SeqNum' -> Bool
infLE (Just n1) (Just n2) = n1 <= n2
infLE _ Nothing = True
infLE _ _ = False

{- | Comare '<' for 'SeqNum''s -}
infLT :: SeqNum' -> SeqNum' -> Bool
infLT n1 n2 = infLE n1 n2 && (n1 /= n2)

{- | Construct a finite 'SeqNum''. -}
snFin :: SeqNum -> SeqNum'
snFin = Just

{- | Construct the infinite 'SeqNum''. -}
snInf :: SeqNum'
snInf = Nothing
