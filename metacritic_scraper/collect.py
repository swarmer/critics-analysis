import json
import os.path
import re

import msgpack


def collect():
    w = os.walk('scraped_data')
    next(w)  # skip root directory

    reviewers = []
    reviews = []

    file_count = 0

    for dirpath, _, filenames in w:
        reviewer_key = re.match(r'\w+/([\w-]+)(?:\?page=\d+)?', dirpath).group(1)

        with open(os.path.join(dirpath, '__meta')) as metafile:
            file_count += 1
            if file_count % 1000 == 0:
                print('Processed %d files' % file_count)

            metadata = json.load(metafile)

            is_publication = (
                metadata['name'] is None and
                metadata['publication_title'] is None and
                metadata['publication_link'] is None
            )
            metadata['key'] = reviewer_key
            metadata['is_publication'] = is_publication

            reviewers.append(metadata)

        for review_path in filenames:
            if review_path == '__meta':
                continue

            with open(os.path.join(dirpath, review_path)) as review_file:
                file_count += 1
                if file_count % 1000 == 0:
                    print('Processed %d files' % file_count)

                review_data = json.load(review_file)
                review_data['reviewer'] = reviewer_key
                review_data['film'] = review_path

                reviews.append(review_data)

    return reviewers, reviews


if __name__ == '__main__':
    reviewers, reviews = collect()

    with open('reviewers.msgpack', 'wb') as reviewers_file:
        msgpack.dump(reviewers_file, reviewers)

    with open('reviews.msgpack', 'wb') as reviews_file:
        msgpack.dump(reviews_file, reviews)
