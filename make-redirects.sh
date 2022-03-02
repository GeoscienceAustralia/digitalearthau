#!/bin/bash

find . -name '*.html' -print0 | while read -d $'\0' name
do
    path="${name#./}"
    cat > "$name" <<EOF
<!DOCTYPE html>
<head>
<meta charset="utf-8">
<title>Redirecting to https://docs.dea.ga.gov.au/</title>
<meta http-equiv="refresh" content="0; URL=https://docs.dea.ga.gov.au/">
</head>
<body>
 Digital Earth Australia documentation has 
 moved to <a href="https://docs.dea.ga.gov.au/">https://docs.dea.ga.gov.au/</a>
</body>
</html>
EOF
done
