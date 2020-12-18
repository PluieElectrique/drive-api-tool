# drive-api-tool

Fetch file metadata from the Google Drive API.

## Setup

You will need a Google Account and a recent version of Python 3. (Tested on 3.8.6.)

If you don't already have a Cloud Platform project with the Drive API enabled, sign in to your Google Account and click the "Enable the Drive API" button in the [Drive API Python quickstart](https://developers.google.com/drive/api/v3/quickstart/python#step_1_turn_on_the). Name the project whatever you want. In the next step, you'll have to "Configure your OAuth client". What you select depends on where you are running this tool.

### Running locally

If you will only be using this tool on your local machine, you can select "Desktop app". Skip the next section and continue with downloading the client configuration.

### Running remotely

If you will be using this tool remotely (e.g. on a VPS), you have a few options:

#### Authorize locally

This is probably the easiest way: authorize locally and copy the credentials/token to the remote server. To do this, select "Desktop app". After completing the rest of the setup, run the tool once on your local machine with an empty file (e.g. `touch empty; python drive_api_tool.py empty unused`) and complete the authorization process. Then, copy `credentials.json` and `token.pickle` to your server.

#### Authorize remotely on a public server

This is more annoying, but still doable. Choose "Web server". You now need to provide a redirect URI. This must be a publicly accessible URI that points to your server. Additionally, Google requires that it not be a public IP address. So, you need a domain name pointing at your server or a workaround:

* Modify your hosts file to point a fake domain name to your server
* Use [xip.io](http://xip.io/), which resolves any domain name like `127.0.0.1.xip.io` to the corresponding IP
* Use a localhost tunnel service like [localtunnel](https://localtunnel.me/) or [ngrok](https://ngrok.com/)

Once you having a working domain, choose the port that you will run the web server on. (The default is `8000`, but you can use `--port` to pass a different port.) Now, enter this exact URI:
```
http://DOMAIN:PORT/
```
Where `DOMAIN` and `PORT` are replaced with your domain and port. It is important that this URI is exact and includes the trailing slash. If the URL does not match, the authorization will fail, and you will have to go into the Cloud Console to fix it.

---

Click the button to "Download client configuration" and save it as `credentials.json`. Ensure that it's in the same directory as this tool, or that you pass it as an argument later (e.g. `--credentials path/to/creds.json`).

Now, install dependencies from `requirements.txt`. For example:

```
pip install -r requirements.txt
```

You can now run the tool with:

```
python drive_api_tool.py id-list.txt out.json
```

If there is no saved token (e.g. this is your first time running the tool) or if your token has expired and can't be refreshed, then you will need to authorize the app. Depending on which option you picked above, you may need to pass a different host and port. For example:

```
python drive_api_tool.py --host example.com --port 8080 id-list.txt out.json
```

After you authorize the tool, you won't need to provide the `--host` or `--port` options again (unless you need to re-authorize). See the next section for details on the authorization process.

## Authorization

If you need to authorize the app, a link will be printed in the console (and opened in a web browser, if available). Go to the page and sign in to your Google Account (it doesn't have to be the same one you used to create the project). Ignore the warning that the app isn't verified and click on "Advanced" and "Go to [project name] (unsafe)" to proceed.

(If you're concerned about security, all this tool does is fetch the metadata of the given IDs. It does not read the metadata of your personal files unless you pass those IDs. The only scope requested is `https://www.googleapis.com/auth/drive.readonly`, so this tool cannot create, modify, or delete files.)

Then, click "Allow" to grant the permissions and "Allow" again to confirm. The tool should now begin to run, and you can now close the window. From now on, you should not need to re-authorize unless `token.pickle` is removed.

(If you picked the "run remotely but authorize locally" option above, remember to copy the credentials and token to your remote server.)

## Output format

The output is a JSON file with the following structure:
```
{
  "metadata": [
    {
      "field1": "value1",
      "field2": "value2",
      ...
    },
    ...
  ],
  "errors": [
    {
      "id": "File ID",
      "code": 404,
      "message": "Some message",
    },
    {
      "id": "File ID",
      "code": 403,
      "message": "Some message",
    },
    ...
  ]
}
```

If there are any errors, a summary of the error codes will be printed at the end. If there are any `403` errors, you are being rate limited. See the next section for more.

## Rate limiting

The default quota for the Drive API is 10,000 queries per 100 seconds. But, this should really be treated as 100 queries/s. Trying to go above this (e.g. 10,000 queries in 10 seconds, then waiting for 90 seconds) results in lots of `403 Rate Limit Exceeded` errors.

If you are being rate limited, ensure that `quota` is not too high. You can see your specific quota in the Google Developer Console: Select your project, "APIs & Services" in the hamburger menu, "Google Drive API" at the bottom of the page, then "Quotas" on the left.  Or, if you get a `403` error with the long message, you can follow the link in it.

If you still need to go faster, you can apply to increase your quota. I haven't tried this, though.

## Options

* `input`: Input file with one Drive API per line. Whitespace is trimmed, and blank and duplicate lines are ignored.
* `output`: Output file containing the fetched metadata and errors as JSON. See [Output format](#output-format) for details.
* `--fields` (default: `*`): Fields to return for each file. The format must follow an [XPath-like syntax](https://developers.google.com/drive/api/v3/fields-parameter#formatting_rules_for_the_fields_parameter). By default, all fields are returned, but for performance, you should only request the fields that you need. The Drive API docs have a [list of all possible fields](https://developers.google.com/drive/api/v3/reference/files).
* `--quota` (default: `100`): Maximum number of queries that can be made per second. For example, a quota of 10,000 requests per 100 seconds is `--quota 100`. See [Rate limiting](#rate-limiting) for more details.
* `--concurrent` (default: `100`): Maximum number of queries that can run at once. This must be less than or equal to `quota`, and it will be set to `quota` if it is higher. For reasonable quotas (e.g. not 100,000 queries per second), it's fine to set `concurrent` equal to `quota`. You should only need to set a lower value if you want to limit bandwidth or memory usage.
* `--indent` (default: `2`): Number of spaces to indent the output JSON by. Set this to 0 to disable indentation.
* `--host` (default: `localhost`): Host for the local auth server. You may need to change this if you are performing authorization on a remote server.
* `--port` (default: `8000`): Port for the local auth server. You may need to change this depending on firewall settings.
* `--credentials` (default: `credentials.json`): Path to JSON file containing client credentials. Follow the steps in [Setup](#setup) if you don't have this file.
* `--token` (default: `token.pickle`): File to store the access and refresh tokens. This saves having to authorize every time you want to run the tool.

## Legal

This program is licensed under the MIT License. See the `LICENSE` file for more information.

This program contains code from:
* [Google Workspace Python Samples](https://github.com/googleworkspace/python-samples) (Apache 2.0)

The implementation of `rate_limited_as_completed` was inspired by:
* "[Making an Unlimited Number of Requests with Python aiohttp + pypeln](https://medium.com/@cgarciae/making-an-infinite-number-of-requests-with-python-aiohttp-pypeln-3a552b97dc95)" by Cristian Garcia (and the other blog posts linked at the start)
* The [implementation of `asyncio.as_completed`](https://github.com/python/cpython/blob/9f004634a2bf50c782e223e2eb386ffa769b901c/Lib/asyncio/tasks.py#L549) in the Python standard library
