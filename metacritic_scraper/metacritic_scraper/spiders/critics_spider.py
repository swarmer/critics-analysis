import json
import os
import pathlib
import re
import urllib.parse

import scrapy


class CriticsSpider(scrapy.Spider):
    data_dir = pathlib.PosixPath('scraped_data')

    name = 'critics'
    start_urls = [
        'http://www.metacritic.com/browse/movies/critic',
    ]

    def save_file(self, path, contents, mode='w'):
        if not isinstance(contents, (bytes, str)):
            contents = json.dumps(contents)

        with open(path, mode) as f:
            f.write(contents)

        self.log('Saved file %s' % path)

    def parse(self, response):
        pagenum_match = re.search(r'\?page=(\d+)', response.url)
        pagenum = int(pagenum_match.group(1)) if pagenum_match else 0
        self.log('Visiting critics batch #%d' % pagenum)

        # visit each critic from this batch
        critic_urls = [
            urllib.parse.urljoin(response.url, critic_url.strip())
            for critic_url in response.css('tr.details_section a::attr(href)').extract()
        ]
        yield from (
            scrapy.Request(critic_url, callback=self.parse_critic_page) for critic_url in critic_urls
        )

        # visit the next batch of critics
        next_urls = [
            urllib.parse.urljoin(response.url, next_url.strip())
            for next_url in response.css('span.flipper.next > a[rel="next"]::attr(href)').extract()
        ]
        yield from (
            scrapy.Request(next_url, callback=self.parse) for next_url in next_urls
        )

    def parse_critic_page(self, response):
        critic_id = response.url.split('/')[-1]
        self.log('Visiting critic: %s' % critic_id)

        pagenum_match = re.search(r'\?page=(\d+)', response.url)
        pagenum = int(pagenum_match.group(1)) if pagenum_match else 0
        self.log('Visiting review batch #%d' % pagenum)

        name = response.css('h1.critic_title::text').extract_first()
        publication_link = response.css('div.publication_title > a::attr(href)').extract_first()
        publication_title = response.css('div.publication_title > a::text').extract_first()

        try:
            os.mkdir(self.data_dir / critic_id)
        except FileExistsError:
            pass

        self.save_file(
            self.data_dir / critic_id / '__meta',
            {
                'name': name,
                'publication_link': publication_link,
                'publication_title': publication_title,
            },
        )

        # save each review from this batch
        reviews = response.css('.review_wrap')
        for review in reviews:
            score = review.css('.review_product_score.brief_critscore > span.metascore_w::text').extract_first()
            movie_link = review.css('.review_product > a::attr(href)').extract_first()
            movie_title = review.css('.review_product > a::text').extract_first()
            review_publication_title = review.css('.review_action.publication_title::text').extract_first()
            review_post_date = review.css('.review_action.post_date::text').extract_first()
            review_link = review.css('.review_action.full_review > a::attr(href)').extract_first()

            movie_id = movie_link.split('/')[-1]

            self.save_file(
                self.data_dir / critic_id / movie_id,
                {
                    'score': score,
                    'movie_link': movie_link,
                    'movie_title': movie_title,
                    'pub_title': review_publication_title,
                    'date': review_post_date,
                    'review_link': review_link,
                },
            )

        # visit the next batch of reviews
        next_urls = [
            urllib.parse.urljoin(response.url, next_url.strip())
            for next_url in response.css('span.flipper.next > a[rel="next"]::attr(href)').extract()
        ]
        yield from (
            scrapy.Request(next_url, callback=self.parse_critic_page) for next_url in next_urls
        )
