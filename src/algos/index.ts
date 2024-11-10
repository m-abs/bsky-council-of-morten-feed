import { AppContext } from '../config'
import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import * as counselOfMorten from './council-of-morten'
import * as blaalys from './blaalys'

type AlgoHandler = (ctx: AppContext, params: QueryParams) => Promise<AlgoOutput>

const algos: Record<string, AlgoHandler> = {
  [counselOfMorten.shortname]: counselOfMorten.handler,
  [blaalys.shortname]: blaalys.handler,
}

export default algos
