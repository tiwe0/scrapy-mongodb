import datetime
import logging

import six
from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import ReadPreference

from scrapy.exporters import BaseItemExporter
from scrapy.exceptions import CloseSpider


def not_set(string):
    """Check if a string is None or ''.

    :returns: bool - True if the string is empty
    """
    if string is None:
        return True
    elif string == '':
        return True
    return False


class MongoDBPipeline(BaseItemExporter):
    """MongoDB pipeline."""

    # Default options
    config = {
        'uri': 'mongodb://localhost:27017',
        'fsync': False,
        'write_concern': 0,
        'database': 'scrapy-mongodb',
        'collection': 'items',
        'separate_collections': False,
        'replica_set': None,
        'unique_key': None,
        'buffer': None,
        'append_timestamp': False,
        'stop_on_duplicate': 0,
    }

    # Item buffer
    current_item = 0
    item_buffer = []

    # Duplicate key occurence count
    duplicate_key_count = 0

    def __init__(self, **kwargs):
        """Constructor."""
        super(MongoDBPipeline, self).__init__(**kwargs)
        self.logger = logging.getLogger('scrapy-mongodb-pipeline')
        self.created_index = False

    def load_spider(self, spider):
        self.crawler = spider.crawler
        self.settings = spider.settings

        # Versions prior to 0.25
        if not hasattr(spider, 'update_settings') and hasattr(spider, 'custom_settings'):
            self.settings.setdict(spider.custom_settings or {}, priority='project')

    def open_spider(self, spider):
        self.load_spider(spider)

        # Configure the connection
        self.configure()

        if self.config['replica_set'] is not None:
            connection = MongoClient(
                self.config['uri'],
                replicaSet=self.config['replica_set'],
                w=self.config['write_concern'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY_PREFERRED)
        else:
            # Connecting to a stand alone MongoDB
            connection = MongoClient(
                self.config['uri'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY)

        # Set up the database
        self.database = connection[self.config['database']]
        self.collections = {'default': self.database[self.config['collection']]}

        self.logger.info(u'Connected to MongoDB {0}, using "{1}"'.format(
            self.config['uri'],
            self.config['database']))

        # Get the duplicate on key option
        if self.config['stop_on_duplicate']:
            tmpValue = self.config['stop_on_duplicate']

            if tmpValue < 0:
                msg = (
                    u'Negative values are not allowed for'
                    u' MONGODB_STOP_ON_DUPLICATE option.'
                )

                self.logger.error(msg)
                raise SyntaxError(msg)

            self.stop_on_duplicate = self.config['stop_on_duplicate']

        else:
            self.stop_on_duplicate = 0

    def configure(self):
        """Configure the MongoDB connection."""
        # Handle deprecated configuration
        if not not_set(self.settings['MONGODB_HOST']):
            self.logger.warning(
                u'DeprecationWarning: MONGODB_HOST is deprecated')
            mongodb_host = self.settings['MONGODB_HOST']

            if not not_set(self.settings['MONGODB_PORT']):
                self.logger.warning(
                    u'DeprecationWarning: MONGODB_PORT is deprecated')
                self.config['uri'] = 'mongodb://{0}:{1:i}'.format(
                    mongodb_host,
                    self.settings['MONGODB_PORT'])
            else:
                self.config['uri'] = 'mongodb://{0}:27017'.format(mongodb_host)

        if not not_set(self.settings['MONGODB_REPLICA_SET']):
            if not not_set(self.settings['MONGODB_REPLICA_SET_HOSTS']):
                self.logger.warning(
                    (
                        u'DeprecationWarning: '
                        u'MONGODB_REPLICA_SET_HOSTS is deprecated'
                    ))
                self.config['uri'] = 'mongodb://{0}'.format(
                    self.settings['MONGODB_REPLICA_SET_HOSTS'])

        # Set all regular options
        options = [
            ('uri', 'MONGODB_URI'),
            ('fsync', 'MONGODB_FSYNC'),
            ('write_concern', 'MONGODB_REPLICA_SET_W'),
            ('database', 'MONGODB_DATABASE'),
            ('collection', 'MONGODB_COLLECTION'),
            ('separate_collections', 'MONGODB_SEPARATE_COLLECTIONS'),
            ('replica_set', 'MONGODB_REPLICA_SET'),
            ('unique_key', 'MONGODB_UNIQUE_KEY'),
            ('buffer', 'MONGODB_BUFFER_DATA'),
            ('append_timestamp', 'MONGODB_ADD_TIMESTAMP'),
            ('stop_on_duplicate', 'MONGODB_STOP_ON_DUPLICATE')
        ]

        for key, setting in options:
            if not not_set(self.settings[setting]):
                self.config[key] = self.settings[setting]

        # Check for illegal configuration
        if self.config['buffer'] and self.config['unique_key']:
            msg = (
                u'IllegalConfig: Settings both MONGODB_BUFFER_DATA '
                u'and MONGODB_UNIQUE_KEY is not supported'
            )
            self.logger.error(msg)
            raise SyntaxError(msg)

    def process_item(self, item, spider):
        """Process the item and add it to MongoDB.

        :type item: Item object
        :param item: The item to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """
        item = dict(self._get_serialized_fields(item))

        item = dict((k, v) for k, v in six.iteritems(item) if v is not None and v != "")

        if self.config['append_timestamp']:
            item['scrapy-mongodb'] = {'ts': datetime.datetime.utcnow()}

        if self.config['buffer']:
            self.current_item += 1

            self.item_buffer.append(item)

            if self.current_item == self.config['buffer']:
                self.current_item = 0

                try:
                    self.insert_item(self.item_buffer, spider)
                    return item
                finally:
                    self.item_buffer = []

            return item

        self.insert_item(item, spider)
        return item

    def close_spider(self, spider):
        """Be called when the spider is closed.

        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: None
        """
        if self.item_buffer:
            self.insert_item(self.item_buffer, spider)

    def insert_item(self, item, spider):
        """Process the item and add it to MongoDB.

        :type item: (Item object) or [(Item object)]
        :param item: The item(s) to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: None
        """

        if isinstance(item, list):
            self._insert_many(item, spider)
        else:
            self._insert_one(item, spider)

    def _insert_one(self, item, spider):
        """Process the item and add it to MongoDB.

        :type item: (Item object)
        :param item: The items to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: None
        """

        collection_name, collection = self.get_collection(spider.name)

        if self.config['unique_key'] is None:
            try:
                collection.insert_one(item)
                self.logger.debug(u'Stored item(s) in MongoDB {0}/{1}'.format(
                    self.config['database'], collection_name))

            except errors.DuplicateKeyError:
                self.logger.debug(u'Duplicate key found')
                if (self.stop_on_duplicate > 0):
                    self.duplicate_key_count += 1
                    if (self.duplicate_key_count >= self.stop_on_duplicate):
                        self.crawler.engine.close_spider(
                            spider,
                            'Number of duplicate key insertion exceeded'
                        )

        else:
            filter = {}
            unique_key = self.config['unique_key']

            if not isinstance(unique_key, list):
                unique_key = [unique_key]

            for k in set(unique_key):
                filter[k] = item[k]

            collection.update_one(filter, {'$set': item}, upsert=True)

            self.logger.debug(u'Stored item(s) in MongoDB {0}/{1}'.format(
                self.config['database'], collection_name))

    def _insert_many(self, items, spider):
        """Process the item and add it to MongoDB.

        :type item: [(Item object)]
        :param item: The items to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: None
        """

        collection_name, collection = self.get_collection(spider.name)

        if self.config['unique_key'] is None:
            try:
                collection.insert_many(items)
                self.logger.debug(u'Stored item(s) in MongoDB {0}/{1}'.format(
                    self.config['database'], collection_name))

            except errors.DuplicateKeyError:
                self.logger.debug(u'Duplicate key found')
                if (self.stop_on_duplicate > 0):
                    self.duplicate_key_count += 1
                    if (self.duplicate_key_count >= self.stop_on_duplicate):
                        self.crawler.engine.close_spider(
                            spider,
                            'Number of duplicate key insertion exceeded'
                        )

        else:
            self.logger.warning(f"buffer is conflict with unique_key, trying inserting data one by one.")
            for item in items:
                self._insert_one(item, spider)

    def get_collection(self, name):
        if self.config['separate_collections']:
            collection = self.collections.get(name)
            collection_name = name

            if not collection:
                collection = self.database[name]
                self.collections[name] = collection
        else:
            collection = self.collections.get('default')
            collection_name = self.config['collection']

        assert collection is not None

        # Ensure unique index
        if self.config['unique_key'] and not self.created_index:
            if isinstance(self.config['unique_key'], list):
                collection.create_indexes(self.config['unique_key'], unique=True)
            else:
                collection.create_index(self.config['unique_key'], unique=True)
            self.created_index = True
            self.logger.info(u'Ensuring index for key {0}'.format(
                self.config['unique_key']))
        return (collection_name, collection)

