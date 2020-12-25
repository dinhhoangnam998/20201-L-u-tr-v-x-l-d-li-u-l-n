# -*- coding: utf-8 -*-

# Importing Scrapy Library
import scrapy
import pandas
import re
import os
import json
from kafka import KafkaProducer

# Creating a new class to implement Spide


class AmazonReviewsSpider(scrapy.Spider):
    # Spider name
    name = 'amazon_reviews'

    def start_requests(self):
        dir_path = 'data/asin_list/'
        filenames = [
            "Toys_and_Games_5.json.csv",
            "Luxury_Beauty_5.json.csv",
            "All_Beauty_5.json.csv",
            "Automotive_5.json.csv",
            "Office_Products_5.json.csv",
            "Patio_Lawn_and_Garden_5.json.csv",
            "Prime_Pantry_5.json.csv",
            "Pet_Supplies_5.json.csv",
            "Industrial_and_Scientific_5.json.csv",
            "Magazine_Subscriptions_5.json.csv",
            "Gift_Cards_5.json.csv",
            "Home_and_Kitchen_5.json.csv",
            "AMAZON_FASHION_5.json.csv",
            "Arts_Crafts_and_Sewing_5.json.csv",
            "Sports_and_Outdoors_5.json.csv",
            "Video_Games_5.json.csv",
            "Kindle_Store_5.json.csv",
            "Tools_and_Home_Improvement_5.json.csv",
            "Movies_and_TV_5.json.csv",
            "Musical_Instruments_5.json.csv",
            "Digital_Music_5.json.csv",
            "Appliances_5.json.csv",
            "Software_5.json.csv",
            "CDs_and_Vinyl_5.json.csv",
            "Cell_Phones_and_Accessories_5.json.csv",
            "Grocery_and_Gourmet_Food_5.json.csv",
            "Electronics_5.json.csv",
        ]

        asin_list = []
        for filename in filenames:
            df = pandas.read_csv(dir_path + filename)
            asins = df['asin'].values.tolist()
            asin_list += asins

        urls = []
        for asin in asin_list:
            for i in range(1, 5):
                urls.append(
                    f'https://www.amazon.com/product-reviews/{asin}/?ie=UTF8&reviewerType=all_reviews&pageNumber={i}&sortBy=recent')

        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={
                    'authority': 'www.amazon.com',
                    'cache-control': 'max-age=0',
                    'rtt': '150',
                    'downlink': '10',
                    'ect': '4g',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-user': '?1',
                    'sec-fetch-dest': 'document',
                    'accept-language': 'vi',
                    'cookie': 'session-id=138-6255665-1654405; session-id-time=2082787201l; i18n-prefs=USD; sp-cdn="L5Z9:VN"; csm-hit=tb:s-4AV1FXKH1Z7MRF3XTDDF|1608819281646&t:1608819282343&adb:adblk_no; ubid-main=135-1487796-1570604; session-token=49Qiq/+7Tsy83tNq6/qIXcfvuOvAM24hzNTBouHtlEzBqq2CZ3HTqRDXmoD5boA0P61qI7dLapGhrVESItWRnIbEn5sdNc4sB7I9Ea6jKNrw7hY070Uy8pGUEY5T+n1hucviIZuf8ykwHv3ZJG2otZ6ZOwRCOChafCj9z+7d3+p+qnZtWBK2ixnSmizJUuSRIZIU4t40rDQ9cZUVUZgNGUMlZesoWjTsCRh3Mn2xmRnZgkK9V+aIz+KMiHQyQM0M',
                })

    def parse(self, response):
        asin = re.findall("/product-reviews/(.*)/", response.request.url)[0]

        # Get the Review List
        div_review_list = response.css('#cm_cr-review_list')
        div_reviews = div_review_list.css('.review')

        if len(div_reviews) == 0:
            # with open(f"{asin}.html", 'wb') as f:
            # f.write(response.body)
            yield None

        for div_review in div_reviews:
            name = div_review.css("span.a-profile-name::text").get()
            title = (''.join(div_review.css(
                ".review-title").xpath(".//text()").extract())).strip()
            rating = div_review.css(".review-rating ::text").get()
            date = div_review.css(".review-date::text").re("on (.*)")[0]
            text = (''.join(div_review.css(
                ".review-text-content").xpath(".//text()").extract())).strip()
            vote = div_review.css(".cr-vote-text::text").get()
            verified = div_review.css(
                'span[data-hook="avp-badge"]::text').get()
            reviewer_id = div_review.css(
                ".a-profile").xpath(".//@href").re('/profile/(.*)')[0]

            # Only get review from 2019
            if (int(date[-4:]) < 2019):
                break

            item = {
                "overall": float(rating[0:3]),
                "vote": 0 if not vote else 1 if vote[0:3] == "One" else int(re.findall("\d+", vote)[0]),
                "verified": verified == "Verified Purchase",
                "reviewTime": date,
                "reviewerID": reviewer_id,
                "asin": asin,
                # "style": {
                #     "Size:": " 7.0 oz",
                #     "Flavor:": " Classic Ice Blue"
                # },
                "reviewerName": name,
                "reviewText": text,
                "summary": title,
                # "unixReviewTime": 1304380800
            }

            # Push data to kafka
            producer = KafkaProducer(bootstrap_servers='hoangnam-msi:9092',
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            producer.send('bigdata', value=item)

            yield item
