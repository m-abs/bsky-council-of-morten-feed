export type DatabaseSchema = {
  post: Post
  sub_state: SubState
}

export type Post = {
  uri: string
  cid: string
  isMorten: number
  isQuestion: number
  indexedAt: string
}

export type SubState = {
  service: string
  cursor: number
}
