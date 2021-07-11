# drive-api-tool

Fetch file metadata from the Google Drive API.

Note: On September 13, 2021, Google Drive will [release a security update](https://workspaceupdates.googleblog.com/2021/06/drive-file-link-updates.html) which means that resource keys may be needed to view some files. This project does not support resource keys yet.

## Setup

You will need a Google Account and a recent version of Python 3. (Tested on 3.9.)

Install dependencies from `requirements.txt` (e.g. `pip install -r requirements.txt`).

If you don't already have a Cloud Platform project with the Drive API enabled, follow [this guide](https://developers.google.com/workspace/guides/create-project) (in the second section, be sure to enable the Drive API).

Now, follow [this guide](https://developers.google.com/workspace/guides/create-credentials#create_a_oauth_client_id_credential) to create OAuth credentials. The type of credentials you create depends on where you are running this tool:

* **Locally**: Follow the steps for a "Desktop application". Download the client secret JSON and save it as `credentials.json` in the same directory as the script (if you save it somewhere else, pass it in as an argument, e.g. `--credentials path/to/creds.json`). The first time you run the script, you will be prompted to go through the [authorization process](#authorization) with your Google Account. After that, you won't need to re-authorize until the token expires.
* **Remotely**: You have two options:
    * **Authorize locally**: This is the easy way: authorize locally and copy the credentials/token to your remote server. To do this, follow the above steps. After completing the setup, run the tool on your local machine with an empty file (e.g. `touch empty; python drive_api_tool.py empty unused_ouput`) and complete the [authorization process](#authorization). Then, copy the created `token.pickle` and your `credentials.json` to the server. You can then remotely run the script as normal.
    * **Authorize remotely**: This is more annoying, but still doable. Follow the steps for a "web server app". (For the redirect URL, if you don't have a domain name that points to your server, you might be able to mess with your hosts file or use a localhost tunnel service like [localtunnel](https://localtunnel.me/) or [ngrok](https://ngrok.com/).) Copy the client secret JSON to your server as `credentials.json`. The first time you run the script, you will be prompted with the [authorization process](#authorization) (and will likely need to pass the host and port of your redirect URL, e.g. `--host example.com --port 8080`). After that, you will not need to re-authorize until the token expires (so you can leave off the `--host` and `--port` arguments until then).

After setting up your credentials, you can run the tool with:

```
python drive_api_tool.py id-list.txt out.json
```

`id-list.txt` is a file with one Docs/Drive ID per line and `out.json` is where the metadata will be stored (see [Output format](#output-format)).

## Authorization

If you need to authorize the app, a link will be printed in the console (and opened in a web browser, if available). Go to the page and sign in to your Google Account (it doesn't have to be the same one you used to create the project). Ignore the warning that the app isn't verified and click on "Advanced" and "Go to [project name] (unsafe)" to proceed.

(If you're concerned about security, this tool only fetches the metadata of the given IDs. It does not read the metadata of your personal files unless you pass those IDs. The only scope requested is `https://www.googleapis.com/auth/drive.readonly` [see [this table](https://developers.google.com/drive/api/v3/about-auth#OAuth2Scope
)], so this tool cannot create, modify, or delete files.)

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

If you are being rate limited, ensure that `quota` is not too high. You can see your specific quota in the Google Developer Console: Select your project from the drop-down in the header, "APIs & Services" in the hamburger menu, "Google Drive API" at the bottom of the page, then "Quotas" on the left.  Or, if you get a `403` error with the long message, you can follow the link in it.

If you still need to go faster, you can apply to increase your quota. I haven't tried this, though.

## Options

* `input`: Input file with one Drive ID per line. Whitespace is trimmed, and blank and duplicate lines are ignored.
* `output`: Output file containing the fetched metadata and errors as JSON. See [Output format](#output-format) for details.
* `--fields` (default: the default fields returned by the API): Fields to return for each file. The format must follow an [XPath-like syntax](https://developers.google.com/drive/api/v3/fields-parameter#formatting_rules_for_the_fields_parameter). For performance, you should only request the fields that you need. The Drive API docs have a [list of all possible fields](https://developers.google.com/drive/api/v3/reference/files).
* `--quota` (default: `100`): Maximum number of queries that can be made per second. For example, a quota of 10,000 requests per 100 seconds is `--quota 100`. See [Rate limiting](#rate-limiting) for details.
* `--concurrent` (default: `100`): Maximum number of queries that can run at once. This must be less than or equal to `quota`, and it will be set to `quota` if it is higher. For reasonable quotas (e.g. not 100,000 queries per second), it's fine to set `concurrent` equal to `quota`. You should only need to set a lower value if you want to limit bandwidth or memory usage.
* `--indent` (default: no indenting): Number of spaces to indent the output JSON by.
* `--host` (default: `localhost`): Host for the local auth server. You may need to change this if you are performing [authorization on a remote server](#authorize-remotely-on-a-public-server).
* `--port` (default: `8000`): Port for the local auth server. You may need to change this depending on firewall settings.
* `--credentials` (default: `credentials.json`): Path to JSON file containing client credentials. Follow the steps in [Setup](#setup) if you don't have this file.
* `--token` (default: `token.pickle`): File to store the access and refresh tokens. This prevents having to authorize every time you want to run the tool.

## Legal

This program is licensed under the MIT License. See the `LICENSE` file for more information.

This program contains code from:
* [Google Workspace Python Samples](https://github.com/googleworkspace/python-samples) (Apache 2.0)

The implementation of `rate_limited_as_completed` was inspired by:
* "[Making an Unlimited Number of Requests with Python aiohttp + pypeln](https://medium.com/@cgarciae/making-an-infinite-number-of-requests-with-python-aiohttp-pypeln-3a552b97dc95)" by Cristian Garcia (and the other blog posts linked at the start)
* The [implementation of `asyncio.as_completed`](https://github.com/python/cpython/blob/9f004634a2bf50c782e223e2eb386ffa769b901c/Lib/asyncio/tasks.py#L549) in the Python standard library
