Demo of spark loading and processing csv

# Process test data in dev mode locally

    sbt "run src/test/resources/sample.csv src/test/resources/rules.json"
    
, result will be saved as _output.json_
    
or as test

    sbt test    
    
# Description

All steps implemented with next notices:
1. Task is done with Scala and Spark 2.2.
2. Adding new data types is implemented via registry, where user may add new handlers explicitly 
via calling method `registerType()`. I've spent most time there, 
however I didn't achieved design I could proud.
3. I implemented step 4 with naive approach, however it worked very well with 200MB csv test.
I would be glad to fix issues if you point that my implementation isn't optimal there.
