import argparse
import datetime
import json
import logging
import os
import queue
import shutil
import sys
import time
from functools import partial

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

RATE_LIMIT_START = 95  # calls per minute
RATE_LIMIT_TIME = 55   # seconds for rate limit


def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


class Exporter:
    def __init__(self):
        self._users = None
        self._channels = None
        self._channel_map = None
        self._channel_name_map = None
        self._user_id_map = None
        self._user_nickname_map = None

        self._rate_limit = RATE_LIMIT_START
        self._rate_limit_queue = queue.Queue()

        slack_token = os.environ["SLACK_BOT_TOKEN"]

        self.client = WebClient(token=slack_token)

        now = datetime.datetime.now()
        self.base_directory = 'archives/{}'.format(now.strftime('%d%B%Y'))
        create_directory(self.base_directory)

    @property
    def users(self):
        if self._users is None:
            self._users = self.get_users()
        return self._users

    @property
    def channels(self):
        if self._channels is None:
            self._channels = self.get_channels()
        return self._channels

    @property
    def channel_map(self):
        if self._channel_map is None:
            self._channel_map, self._channel_name_map = self.get_channel_maps()
        return self._channel_map

    @property
    def channel_name_map(self):
        if self._channel_name_map is None:
            self._channel_map, self._channel_name_map = self.get_channel_maps()
        return self._channel_name_map

    @property
    def user_id_map(self):
        if self._user_id_map is None:
            self._user_id_map = self.get_user_id_map()
        return self._user_id_map

    @property
    def user_nickname_map(self):
        if self._user_nickname_map is None:
            self._user_nickname_map = self.get_user_nickname_map()
        return self._user_nickname_map

    def call_with_rate_limit(self, func):
        done = False

        while not done:
            try:
                self.rate_limit()
                logging.debug('Making rate-limited call')
                return func()
            except SlackApiError as exc:
                if exc.response['error'] == 'ratelimited':
                    logging.info('Rate limited. Sleeping for 5 seconds')
                    time.sleep(5)

    def rate_limit(self):
        now = datetime.datetime.now()
        self._rate_limit_queue.put(now)
        queue_size = self._rate_limit_queue.qsize()
        logging.debug("Rate limit queue size: %s", queue_size)

        if queue_size >= RATE_LIMIT_START:
            logging.debug("Rate limit queue at max")
            oldest = self._rate_limit_queue.get()

            time_difference = int((now - oldest).total_seconds())
            logging.debug('Now %s', now)
            logging.debug('Oldest %s', oldest)
            logging.debug('Time difference: %s', time_difference)
            if time_difference < RATE_LIMIT_TIME:
                gap = RATE_LIMIT_TIME - time_difference
                logging.info("Rate limit reached. Need to sleep")
                logging.debug("Gap is %s", gap)
                sleep_time = min(int(gap + 2), 60)
                logging.info("Sleeping for %s seconds", sleep_time)

                while sleep_time > 0:
                    sys.stdout.write(str(sleep_time) + ' ')
                    sys.stdout.flush()
                    sleep_time -= 1
                    time.sleep(1)
                print()

    def write_file(self, object_name, data, append=False, is_json=False, format=False, extension=True):
        mode = 'a' if append else 'w'

        if extension:
            extension = '.json' if is_json else '.txt'
        else:
            extension = ''

        name_parts = object_name.split('/')
        if len(name_parts) > 1:
            object_name = name_parts.pop(-1)
            directory = '/'.join([self.base_directory, *name_parts])
            create_directory(directory)
        else:
            directory = self.base_directory

        filename = '{}/{}{}'.format(directory, object_name, extension)
        logging.debug('Writing %s, is_json=%s, append=%s', filename, is_json, append)

        with open(filename, mode=mode) as outf:
            if is_json:
                kwargs = {}
                if format:
                    kwargs.update(
                        {
                        'indent': 4,
                        'sort_keys': True
                        }
                    )

                json.dump(data, outf, **kwargs)
            else:
                if isinstance(data, list):
                    for line in data:
                        outf.write(json.dumps(line))
                        outf.write('\n')
                else:
                    outf.write(json.dumps(data))
                    outf.write('\n')

    def load_file(self, object_name, is_json=True):
        extension = 'json' if is_json else 'txt'
        filename = '{}/{}.{}'.format(self.base_directory, object_name, extension)

        if os.path.exists(filename):
            logging.debug('Loading %s from %s', object_name, filename)
            with open(filename, mode='r') as inf:
                if is_json:
                    return json.load(inf)
                else:
                    return inf.readlines()
        else:
            logging.debug('%s not found', filename)
            return None

    def get_all(self, func_partial, key=None):
        calls = []
        values = []
        output = [values, calls]

        kwargs = {}
        has_more = True

        while has_more:
            logging.info("Getting next of %s with %s", str(func_partial), kwargs)
            caller = partial(func_partial, **kwargs)
            resp = self.call_with_rate_limit(caller)

            calls.append(resp.data)

            if key:
                values.extend(resp.data[key])

            cursor = resp.data.get('response_metadata', {}).get('next_cursor')
            kwargs['cursor'] = cursor
            if not cursor:
                has_more = False

        return output

    def get_channels(self):
        channels = self.load_file('channels', is_json=True)
        if not channels:
            logging.info("Fetching channels from API")
            convo_types = 'public_channel,private_channel,mpim,im'
            convo_call = partial(self.client.conversations_list, types=convo_types)
            channels, channels_calls = self.get_all(convo_call, 'channels')

            self.write_file('channels', channels, is_json=True)
            self.write_file('channels_calls', channels_calls, is_json=True)

        return channels

    def get_users(self):
        users = self.load_file('users', is_json=True)
        if not users:
            users_partial = partial(self.client.users_list)
            users, users_calls = self.get_all(users_partial, 'members')

            self.write_file('users', users, is_json=True, format=True)
            self.write_file('users_calls', users_calls, is_json=True)

        return users

    def get_channel_maps(self):
        channel_map = {}
        channel_name_map = {}
        for channel in self.channels:
            channel_map['channel_id'] = channel
            name = channel.get('name')
            if name:
                channel_name_map[name] = channel

        self.write_file('channel_map', channel_map, is_json=True)
        self.write_file('channel_name_map', channel_name_map, is_json=True)
        return channel_map, channel_name_map

    def get_user_id_map(self):
        user_map = {}
        for user in self.users:
            user_map[user['id']] = user

        self.write_file('user_id_map', user_map, is_json=True, format=True)
        return user_map

    def get_user_nickname_map(self):
        user_map = {}
        for user in self.users:
            user_map[user['id']] = user

        self.write_file('user_nickname_map', user_map, is_json=True, format=True)
        return user_map

    def get_conversation_members(self, channel_id):
        object_name = 'channel_members/{}'.format(channel_id)
        object_name_call = '{}_call'.format(object_name)
        convo_members = self.load_file(object_name, is_json=True)

        if convo_members is None:
            try:
                func_partial = partial(self.client.conversations_members, channel=channel_id)
                convo_members, convo_members_calls = self.get_all(func_partial, 'members')

                self.write_file(object_name, sorted(convo_members), is_json=True, format=True)
                self.write_file(object_name_call, convo_members_calls, is_json=True)

                logging.debug('Success %s', channel_id)
            except SlackApiError as exc:
                logging.exception(exc)
                logging.info('Channel ID: %s', channel_id)

        return convo_members

    def get_conversation_members_map(self):
        convo_members_map = {}
        convo_members_map_names = {}

        for channel in self.channels:
            channel_id = channel['id']
            num_members = channel.get('num_members')

            if num_members != 0:
                convo_members = self.get_conversation_members(channel_id)
                if convo_members:
                    convo_members_map[channel['id']] = convo_members
                    nicknames = []
                    for user_id in convo_members:
                        user = self.user_id_map.get(user_id)
                        if user:
                            nicknames.append(user['profile']['real_name'])
                        else:
                            nicknames.append(user_id)
                    convo_members_map_names[channel['id']] = nicknames
            else:
                logging.debug('No members for Channel ID: %s, Channel Name %s', channel_id, channel.get('name'))

        self.write_file('convo_members_map', convo_members_map, is_json=True, format=True)
        self.write_file('convo_members_map_names', convo_members_map_names, is_json=True, format=True)
        return convo_members_map

    def _get_conversation(self, channel, directory):
        channel_id = channel['id']
        has_more = True
        kwargs = {}

        name = channel.get('name', channel_id)

        func_partial = partial(self.client.conversations_history, channel=channel_id)
        reply_func_partial = partial(self.client.conversations_replies, channel=channel_id)

        num_calls = 0

        while has_more:
            logging.debug("Getting next of %s with %s", str(func_partial), kwargs)
            logging.debug("Channel: %s (%s)", name, channel_id)
            caller = partial(func_partial, **kwargs)
            resp = self.call_with_rate_limit(caller)
            data = resp.data

            filename = '{}/{}'.format(directory, channel_id)
            self.write_file(filename, data, append=True)

            for message in data.get('messages', []):
                if message.get('reply_count', 0) > 0:
                    logging.debug("Found message with reply_count %s", message['reply_count'])
                    ts = message.get('ts')
                    thread_ts = message.get('thread_ts')

                    if ts and thread_ts and ts == thread_ts:
                        logging.debug("Found thread parent")
                        # This is a parent
                        # Get the replies
                        this_reply_func_partial = partial(reply_func_partial, ts=ts)
                        _, calls = self.get_all(this_reply_func_partial)
                        reply_filename = '{}/{}'.format(directory, 'reply_{}'.format(ts))
                        for call in calls:
                            self.write_file(reply_filename, call, append=True)

            cursor = data.get('response_metadata', {}).get('next_cursor')
            kwargs['cursor'] = cursor
            if not cursor:
                has_more = False

            num_calls += 1
            logging.info('Number of calls for %s: %s', name, num_calls)

    def get_conversation(self, channel, directory):
        channel_id = channel['id']

        done_filename = '{}/{}'.format(directory, 'done')
        full_directory = '{}/{}'.format(self.base_directory, directory)
        full_filename = '{}/{}'.format(full_directory, 'done')
        logging.debug('Full filename %s', full_filename)

        if not os.path.exists(full_filename):
            if os.path.exists(full_directory):
                logging.info('Partial conversation exists. Removing %s', full_directory)
                shutil.rmtree(full_directory)
            logging.info('Getting conversation for %s', channel_id)
            self._get_conversation(channel, directory)
            self.write_file(done_filename, '', extension=False)
        else:
            logging.info('Already done, skipping')

    def get_channel_messages(self):
        filename = 'channels_to_export.json'

        if not os.path.exists(filename):
            print('')
            print("*" * 100)
            print("A file called '{}' with a JSON array of channel names to exported is needed".format(filename))
            print('Unable to export channel messages')
            print("*" * 100)
            print('')
            input('Hit return to continue')
            return

        with open(filename) as inf:
            channels_to_export = json.load(inf)

        problem_names = []

        for channel_name in channels_to_export:
            logging.info('Working on %s', channel_name)

            channel = self.channel_name_map.get(channel_name)
            if not channel:
                problem_names.append(channel_name)
                continue

            directory = 'conversations/channels/{}'.format(channel_name)

            self.get_conversation(channel, directory)

    def get_private_messages(self):
        nameless = []

        for channel in self.channels:
            if not channel.get('name'):
                nameless.append(channel)

        for channel in sorted(nameless, key=lambda x: x['id']):
            channel_id = channel['id']
            directory = 'conversations/private/{}'.format(channel_id)

            self.get_conversation(channel, directory)

    def generate_channels_to_export_template(self):
        channel_names = []
        for channel in self.channels:
            channel_name = channel.get('name')
            if channel_name:
                channel_names.append(channel_name)

        with open('channels_to_export_template.json', 'w') as outf:
            json.dump(
                sorted(channel_names),
                outf,
                indent=4,
                sort_keys=True
            )


def is_yes(value):
    return str(value).lower() in ('yes', 'y')


def prompt_for_channel_names():
    message = """
Copy the newly generated 'channels_to_export_template.json'
file to a new file named 'channels_to_export.json'.
Edit the list down to just the channels you would like to export.
Then hit enter.
"""
    print(message)
    input("Press Enter to continue...")


def main():
    if not os.environ.get('SLACK_BOT_TOKEN'):
        print("You must set the 'SLACK_BOT_TOKEN' environment variable")
        sys.exit(1)

    exporter = Exporter()

    exporter.get_conversation_members_map()

    if is_yes(input('Do you want to export channel messages? [y/N]')):
        exporter.generate_channels_to_export_template()
        prompt_for_channel_names()
        exporter.get_channel_messages()

    if is_yes(input('Do you want to export private messages? [y/N]')):
        exporter.get_private_messages()


def parse_args():
    parser = argparse.ArgumentParser(description='Export data from Slack')
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.info('Setting log level to debug')
        logging.getLogger().setLevel(logging.DEBUG)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
    parse_args()
    main()