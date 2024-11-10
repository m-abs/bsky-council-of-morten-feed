import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

function makeMortenCacheKey(author: string) {
  return `isMorten:${author}`
}

// Question hashtags regexp
const questionHashtags = [
  /\B#blålys\b/iu,
  /\B#blåhjerne\b/iu,
  /\B#twitterhjerne\b/iu,
]

// Cache TTL for Morten check
const mortenCacheTTL = 60 * 60 * 24 * 1000

/**
 * Check if a post has one of the question hashtags
 */
function isQuestionPost(post: string) {
  return questionHashtags.some((hashtag) => hashtag.test(post))
}

// List of allowed languages for the Council of Mortens feed.
const mortenLangs = ['da', 'en', 'no', 'sv', 'fi', 'is', 'fo', 'kl']

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) {
      return
    }

    const ops = await getOpsByType(evt)

    const isNotMorten = new Set<string>()
    const isMorten = new Set<string>()

    // List of author Dids of created posts
    const createAuthorDids = ops.posts.creates
      .filter((create) => mortenLangs.some((langCode) => create.record.langs?.includes(langCode)))
      .map((create) => create.author)
      .filter((authorDid) => authorDid)

    if (createAuthorDids.length > 0) {
      // Check if create authors are named Morten
      const authors = new Set(createAuthorDids)

      // List of author Dids that need to be checked if they are named Morten
      const needsCheckForMorten = new Set<string>()

      for (const author of authors) {
        if (!author) {
          continue
        }

        // Check the memcache if we have already checked if the author is Morten.
        const authorIsMorten = await this.memCache.get<boolean>(makeMortenCacheKey(author))
        if (authorIsMorten === true) {
          isMorten.add(author)
        } else if (authorIsMorten === false) {
          isNotMorten.add(author)
        } else {
          needsCheckForMorten.add(author)
        }
      }

      if (isNotMorten.size > 0) {
        console.log("Cached is not Morten - ", ...isNotMorten)
      }

      if (isMorten.size > 0) {
        console.log("Cached is Morten - ", ...isMorten)
      }

      if (needsCheckForMorten.size > 0) {
        // Fetch the profiles of the authors that need to be checked, if they are named Morten
        const authorProfiles = (await this.agent.getProfiles({ actors: [...needsCheckForMorten] })).data.profiles

        const missIsMorten = new Map<string, string>()
        const missIsNotMorten = new Map<string, string>()
        for (const profile of authorProfiles) {
          if (!profile.displayName) {
            isNotMorten.add(profile.did)
            continue
          }

          const authorIsMorten = profile.displayName.toLocaleLowerCase().includes('morten')

          if (authorIsMorten) {
            isMorten.add(profile.did)
            missIsMorten.set(profile.did, profile.displayName)
          } else {
            isNotMorten.add(profile.did)
            missIsNotMorten.set(profile.did, profile.displayName)
          }

          await this.memCache.set(makeMortenCacheKey(profile.did), authorIsMorten, mortenCacheTTL)
        }

        if (missIsNotMorten.size > 0) {
          console.log("Missed is not Morten - ", ...missIsNotMorten)
        }

        if (missIsMorten.size > 0) {
          console.log("Missed is Morten - ", ...missIsMorten)
        }
      }
    }

    const postsToCreate = ops.posts.creates
      .filter((create) => isMorten.has(create.author) || isQuestionPost(create.record.text))
      .map((create) => {
        return {
          uri: create.uri,
          cid: create.cid,
          isMorten: isMorten.has(create.author) ? 1 : 0,
          isQuestion: isQuestionPost(create.record.text) ? 1 : 0,
          indexedAt: new Date().toISOString(),
        }
      })

    const postsToDelete = [
      ...ops.posts.deletes,
    ].map((del) => del.uri)

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }

    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
