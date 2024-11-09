import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

function makeMortenCacheKey(author: string) {
  return `isMorten:${author}`;
}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)

    const isNotMorten = new Set<string>();
    const isMorten = new Set<string>();

    const createAuthorDids = ops.posts.creates
      .filter((create) => create.record.langs?.includes('da') === true)
      .map((create) => create.author).filter((author) => author !== null);

    if (createAuthorDids.length > 0) {
      const authors = new Set(createAuthorDids);

      const needsCheck = new Set<string>();

      for (const author of authors) {
        if (!author) {
          continue;
        }

        const authorIsMorten = await this.memCache.get<boolean>(makeMortenCacheKey(author));
        if (authorIsMorten === true) {
          isMorten.add(author);
          console.log("morten", author);
        } else if (authorIsMorten === false) {
          isNotMorten.add(author);
          console.log("not morten", author);
        } else {
          needsCheck.add(author);
        }
      }

      if (needsCheck.size > 0) {
        const authorProfiles = (await this.agent.getProfiles({ actors: [...needsCheck] })).data.profiles;

        for (const profile of authorProfiles) {
          const authorIsMorten = profile.displayName?.toLocaleLowerCase().includes('morten');

          if (authorIsMorten) {
            isMorten.add(profile.did);
            console.log("morten", profile.displayName);
          } else {
            isNotMorten.add(profile.did);
            console.log("not morten", profile.displayName);
          }

          await this.memCache.set(makeMortenCacheKey(profile.did), authorIsMorten, 60 * 60 * 24);
        }
      }
    }

    const postsToCreate = ops.posts.creates
      .filter((create) => isMorten.has(create.author))
      .map((create) => {
        // console.log(inspect(create, { depth: 5 }));
        return {
          uri: create.uri,
          cid: create.cid,
          isMorten: 1,
          indexedAt: new Date().toISOString(),
        }
      })

    const postsToDelete = [
      ...ops.posts.deletes,
      ...ops.posts.creates.filter((create) => isNotMorten.has(create.author))
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
