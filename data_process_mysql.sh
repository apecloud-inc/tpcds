sed -i -e 's/^|/\\N|/' -e 's/||/|\\N|/g' -e 's/||/|\\N|/g' -e 's/|$/|/' ./data/*.dat