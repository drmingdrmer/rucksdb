#!/bin/bash
git push && gh run watch $(gh run list --limit 1 --json databaseId --jq '.[0].databaseId') --interval 10
