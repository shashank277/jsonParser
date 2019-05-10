# jsonParser
Its a Spark based custom json to csv convertor, must be used of learning purpose only and cannot be used for any evil causes.

PLease copy the hadoop and CustomJsonParser folder in your system C drive.

Command line arguments:
1. Input file name or directory name with .*json wild card.
2. SQL query for the required attributes(for which flat csv is required).
3. Output directory path.

Output:

All outputs will be genrated in the output directory provided.

Note: 

1. Script must have access to folders.
2. Json file must in UTF-8 encoding, else UTF-8 char set will get converted to junk values.
3. Each json is expected to have single root tag(1 root record per file). Any level of nesting is allowed, Else pre parser code needs to be updated to convert file in proper inline json format.

Sample command line arguments:

Sample 1: When there are different json files with similar structure.

"C:\\CustomJsonParser\\json\\*.json" "select risks.caseReferenceNumber,risks.category,risks.id,risks.lastUpdateTimeStamp,risks.lastUpdateUser,risks.result,risks.ruleId,risks.score from (select explode(data.risks) as risks from target_table) risks" "C:\\CustomJsonParser\\"

Sample 2: When there is single json file.

"C:\\CustomJsonParser\\json\\dummy.json" "select risks.caseReferenceNumber,risks.category,risks.id,risks.lastUpdateTimeStamp,risks.lastUpdateUser,risks.result,risks.ruleId,risks.score from (select explode(data.risks) as risks from target_table) risks" "C:\\CustomJsonParser\\"
