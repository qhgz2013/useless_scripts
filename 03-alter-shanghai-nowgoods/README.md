# Alter shanghai nowgoods

A script that used for monitoring figures on [Alter Shanghai website](http://alter-shanghai.cn/). It's currently running on my Raspberry Pi. QQ e-mail is required (which is hard-coded in this script, you need to obtain an authorization code to login). The sample configuration file is `sample.json`, it contains a sample that detect the Jeanne d'Arc figure (character in mobile game Fate/Grand Order). Copy and rename to `config.json` for your modification.

Additional dependencies: `requests` and `beautifulsoup4`.

Workflow:
1. Retrieve and parse the HTML.
2. Search your keywords from the configuration, and send e-mail if one of the keywords found.
3. Wait and go back to step 1. (So multiple e-mails will be sent if you don't kill this process, which is designed to make sure that you've already known it)
