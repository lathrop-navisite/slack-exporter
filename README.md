# slack-exporter
Export Slack

## Setup

#### Install Necessary Packages
`pip install -r requirements.txt`

#### Set Up Slack App

1. While logged in, visit [https://api.slack.com/apps](https://api.slack.com/apps)
1. Click the "Create New App" in the top right
1. Give it whatever name you'd like, such as "My Exporter"
1. Choose the appropriate "Development Slack Workspace"
1. Once created, click "OAuth & Permissions" on the left
1. Scroll down to the "Scopes" section
1. Under "User Token Scopes" add the following scopes:
    * `channels:history`
    * `channels:read`
    * `files:read`
    * `groups:history`
    * `groups:read`
    * `im:history`
    * `im:read`
    * `mpim:history`
    * `mpim:read`
    * `users:read`
    * `emoji:read`
1. Scroll back to the top and click "Install to Workspace" or "Reinstall to Workspace" if this was a pre-existing app
1. On the next screen, click "Allow"
1. Copy the "User OAuth Token"
1. Run the exporter script from the top level of this repo with the token set as the environment variable
    * `SLACK_BOT_TOKEN='<TOKEN>' python exporter.py`
    * Use the `-v` option to the script to get a more verbose output
1. An `archives` directory will be created. Underneath will be a dated folder with the exported data.
