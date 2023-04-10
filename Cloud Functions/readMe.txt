1. I have already run the job for Map reduce here and buckets already exist with II and WC file. Test the UI part and re run if you want to test the 
functionality of Faas functions (Was doubtful if unauthenticated functions of 1 project can access buckets of other projects)
2. To run the entire Mapreduce (assuming functions are deployed) end to end, run: python3 start.py

3. To run for a new file, just change name in config.json and ensure that file exists in local directory, re run python3 start.py

4. To deploy functions cd into respective folder and run the sh file inside that function folder.

5. To test the II and word count, open folder front-end and click on the HTML page given there. 
If map reduce is completed once you enter word and click on search button it will display results on next page. To check new word, press back and search again.

6. To delete everything including functions, run sh delete.sh in A4_P2. To re run logic redeploy each function individually.

