import argparse
import httpx
import logging
import magic
import os
import rich.progress
from atproto import CAR, Client, models
from atproto_core.cid import CID
from atproto_client.request import Request
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from functools import partial
from pathlib import Path

logging.basicConfig(filename='skeeter_deleter.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

class PostQualifier(models.AppBskyFeedDefs.PostView):
    """
    Wrapper around ATProto PostView with minimal helpers needed by this script.
    """
    def is_self_liked(self, self_likes) -> bool:
        """
        Check if this post appears in the list of self-like records extracted from the archive.
        `self_likes` is expected to be a list of like records (dictionaries) from the archive.
        """
        return self.uri in [post['subject']['uri'] for post in self_likes]
    
    def __init__(self, client : Client):
        super(PostQualifier, self).__init__()
        self._init_PostQualifier(client)
    
    def _init_PostQualifier(self, client : Client):
        self.client = client

    def remove(self):
        """
        Remove a repost or delete an authored post
        """
        if self.author.did != self.client.me.did:
            try:
                logging.info(f"Removing repost: {self.viewer.repost}")
                self.client.unrepost(self.viewer.repost)
            except httpx.HTTPStatusError as e:
                logging.error(f"HTTP error occurred during unreposting: {e}")
            except Exception as e:
                logging.error(f"An error occurred during unreposting: {e}")
        else:
            try:
                logging.info(f"Removing post: {self.uri}")
                self.client.delete_post(self.uri)
            except httpx.HTTPStatusError as e:
                logging.error(f"HTTP error occurred during deletion: {e}")
            except Exception as e:
                logging.error(f"An error occurred during deletion: {e}")

    @staticmethod
    def cast(client : Client, post : models.AppBskyFeedDefs.PostView):
        """
        Cast a post to a PostQualifier instance.
        """
        post.__class__ = PostQualifier
        post._init_PostQualifier(client)
        return post
    
@dataclass
class Credentials:
    login: str
    password: str

    dict = asdict


class RequestCustomTimeout(Request):
    def __init__(self, timeout: httpx.Timeout = httpx.Timeout(120), *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = httpx.Client(follow_redirects=True, timeout=timeout)


class SkeeterDeleter:
    @staticmethod
    def chunker(seq, size : int):
        """
        Break a iterable into segments of a given size
        """
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    
    @staticmethod
    def extract_feed_item(archive, block):
        """
        Converts feed items from the repo with various structures into consistent blocks
        """
        if '$type' in block:
            return block
        elif 'e' in block and len(block['e']) > 0:
            return archive.blocks.get(CID.decode(block['e'][0]['v']))
        else:
            return block

    def _is_older_than_days(self, post, days: int) -> bool:
        """
        Return True if the post is older than `days` days.
        Handles created timestamps either as strings (ISO) or datetime objects.
        If the created time cannot be determined, returns False (conservative).
        """
        created = None
        # Try common attribute names first
        try:
            created = getattr(post.record, 'created_at', None) or getattr(post.record, 'createdAt', None)
        except Exception:
            created = None

        # If still not found, try treating record as dict-like
        if created is None:
            try:
                created = post.record.get('createdAt') or post.record.get('created_at')
            except Exception:
                created = None

        if created is None:
            return False

        # Parse string timestamps robustly
        if isinstance(created, str):
            try:
                if created.endswith('Z'):
                    created_dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
                else:
                    created_dt = datetime.fromisoformat(created)
            except Exception:
                # Could not parse timestamp
                return False
        elif isinstance(created, datetime):
            created_dt = created
        else:
            return False

        # Ensure timezone-aware
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=timezone.utc)

        return (datetime.now(timezone.utc) - created_dt) > timedelta(days=days)

    def gather_self_liked_posts(self, repo, **kwargs) -> list[PostQualifier]:
        """
        From the archived repo, find like records created by this account (the archive contains
        your own likes), identify those likes that reference your own posts (self-likes),
        fetch the corresponding posts via the API, and return the subset authored by you
        that are older than 3 days.

        Returns:
            list[PostQualifier]: posts authored by me that I have self-liked and are older than 3 days
        """
        archive = CAR.from_bytes(repo)
        # Extract all like records from the archive (these are likes made by this account)
        likes = list(map(partial(self.extract_feed_item, archive),
                         filter(lambda x: 'app.bsky.feed.like' in str(x),
                                [archive.blocks.get(cid) for cid in archive.blocks])))
        
        # Likes that reference posts whose subject URI contains my DID (i.e., likes on my posts)
        self_like_records = list(filter(lambda x: archive.blocks.get(x['subject']['cid']),
                                       filter(lambda x : self.client.me.did in x['subject']['uri'], likes)))
        
        posts_to_delete = []
        for batch in self.chunker(self_like_records, 25):
            try:
                posts = self.client.get_posts(uris=[x['subject']['uri'] for x in batch])
                # Keep only posts authored by me (safety check) AND older than 3 days
                posts_to_delete.extend(
                    [p for p in map(partial(PostQualifier.cast, self.client), posts.posts)
                     if p.author.did == self.client.me.did and self._is_older_than_days(p, 3)]
                )
            except httpx.HTTPStatusError as e:
                logging.error(f"An HTTP error occured while fetching self-liked posts: {e}")
            except Exception as e:
                logging.error(f"An error occured while fetching self-liked posts: {e}")
        return posts_to_delete

    def batch_delete_posts(self) -> None:
        logging.info(f"Deleting {len(self.to_delete)} post{'' if len(self.to_delete) == 1 else 's'}")
        if self.verbosity > 0:
            print(f"Deleting {len(self.to_delete)} post{'' if len(self.to_delete) == 1 else 's'}")
        for post in rich.progress.track(self.to_delete):
            logging.info(f"Deleting: {post.record.text} on {post.record.created_at}, CID: {post.cid}")
            if self.verbosity == 2:
                print(f"Deleting: {post.record.text} on {post.record.created_at}, CID: {post.cid}")
            post.remove()
            
    def archive_repo(self, now, **kwargs):
        repo = self.client.com.atproto.sync.get_repo(params={'did': self.client.me.did})
        clean_user_did = self.client.me.did.replace(":", "_")
        Path(f"archive/{clean_user_did}/_blob/").mkdir(parents=True, exist_ok=True)
        print("Archiving posts...")
        clean_now = now.isoformat().replace(':','_')
        with open(f"archive/{clean_user_did}/bsky-archive-{clean_now}.car", "wb") as f:
            f.write(repo)

        cursor = None
        print("Downloading and archiving media...")
        blob_cids = []
        while True:
            blob_page = self.client.com.atproto.sync.list_blobs(params={'did': self.client.me.did, 'cursor': cursor})
            blob_cids.extend(blob_page.cids)
            cursor = blob_page.cursor
            if not cursor:
                break
        for cid in rich.progress.track(blob_cids):
            blob = self.client.com.atproto.sync.get_blob(params={'cid': cid, 'did': self.client.me.did})
            type = magic.from_buffer(blob, 2048)
            ext = ".jpeg" if type == "image/jpeg" else ""
            with open(f"archive/{clean_user_did}/_blob/{cid}{ext}", "wb") as f:
                if self.verbosity == 2:
                    print(f"Saving {cid}{ext}")
                f.write(blob)

        return repo

    def __init__(self,
                 credentials : Credentials,
                 fixed_likes_cursor : str=None,
                 verbosity : int=0,
                 autodelete : bool=False):
        self.client = Client(request=RequestCustomTimeout())
        self.client.login(**credentials.dict())

        params = {
            'fixed_likes_cursor': fixed_likes_cursor,
            'now': datetime.now(timezone.utc),
        }
        self.verbosity = verbosity
        self.autodelete = autodelete

        repo = self.archive_repo(**params)

        # Find posts I have self-liked (i.e., likes I created that reference my own posts)
        self_liked_posts = self.gather_self_liked_posts(repo, **params)
        print(f"Found {len(self_liked_posts)} self-liked post{'' if len(self_liked_posts) == 1 else 's'} to delete (only including posts older than 3 days).")

        # Only delete authored posts that you self-liked
        self.to_delete = self_liked_posts

    def delete(self):
        n_delete = len(self.to_delete)
        prompt = None
        while not self.autodelete and prompt not in ("Y", "n"):
            prompt = input(f"""
Proceed to delete {n_delete} post{'' if n_delete == 1 else 's'}? WARNING: THIS IS DESTRUCTIVE AND CANNOT BE UNDONE. Y/n: """)
        if self.autodelete or prompt == "Y":
            self.batch_delete_posts()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--fixed-likes-cursor", help="""A complex setting. ATProto pagination through is awkward, and
it will page through the entire history of your account even if there are no likes to be found. This can make the process take
a long time to complete. If you have already purged likes, it's possible to simply set a token at a reasonable point in the recent
past which will terminate the search. To list the tokens, run -vv mode. Tokens are short alphanumeric strings. Default empty.""",
default="")
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument("-v", "--verbose", help="""Show more information about what is happening.""",
                           action="store_true")
    verbosity.add_argument("-vv", "--very-verbose", help="""Show granular information about what is happening.""",
                           action="store_true")
    parser.add_argument("-y", "--yes", help="""Ignore warning prompts for deletion. Necessary for running in automation.""",
                        action="store_true", default=False)
    args = parser.parse_args()

    creds = Credentials(os.environ["BLUESKY_USERNAME"],
                        os.environ["BLUESKY_PASSWORD"])
    verbosity = 0
    if args.verbose:
        verbosity = 1
    elif args.very_verbose:
        verbosity = 2
    params = {
        'fixed_likes_cursor': args.fixed_likes_cursor,
        'verbosity': verbosity,
        'autodelete': args.yes
    }

    sd = SkeeterDeleter(credentials=creds, **params)
    sd.delete()
