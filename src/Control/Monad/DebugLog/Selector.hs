module Control.Monad.DebugLog.Selector
  ( DebugAtom (..)
  , Selector
  , selectorMatch
  , parseDebugSelector
  , ParseError
  , prettySelector
  ) where

import Text.Parsec
import Data.List (intersperse)

data DebugAtom
  = DebugAll
  | DebugTag String
  deriving (Show,Eq,Ord)

prettySelector :: Selector -> String
prettySelector = mconcat . intersperse "::" . map prettyAtom

prettyAtom DebugAll = "*"
prettyAtom (DebugTag s) = s

type Selector = [DebugAtom]

selectorMatch :: Selector -> [String] -> Bool
selectorMatch ss ts = case (ss,ts) of
  ([],[]) -> True
  ([],_) -> False
  (_,[]) -> False
  (DebugTag s : _, t : _) | s /= t -> False
  (_ : ss', _ : ts') -> selectorMatch ss' ts'

parseDebugSelector :: String -> Either ParseError Selector
parseDebugSelector = parse dsP ""

dsP :: Parsec String () Selector
dsP = sepBy atomP (string "::")

atomP :: Parsec String () DebugAtom
atomP = (DebugAll <$ string "*") <|> (DebugTag <$> many1 alphaNum)
