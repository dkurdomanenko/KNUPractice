# KNUPractice
My practice in university

During study practice I had to write a script that converts csv file into pandas data frame, clears the DF and save data in DB. First of all I had to look at data in csv file in order to decide which technique use to create a DF. The DF was created, after that it had to be cleaned. There were a lot of cells with missed values. All rows with missed values had to be deleted except if cell is not in admissible column. There weren’t any "dangerous" NA values, so I could continue. But one column had wrong format of data, so I had to delete rows with wrong format. One column had urls and not all of them were responding. So I had to delete rows with the urls weren’t responding. And finally the DF was clean. Every URL is a way to download file, I had to download all of them, extract HTML content from them and save HMTL in My SQL db. 
