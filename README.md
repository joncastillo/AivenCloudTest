"# AivenCloudTest" 

**Evaluation of Aiven.io's Services**

    Usage sample: python AivenCloudTest.py --url https://www.cnn.com --regex "news.+\." --delay 30
           This samples https://www.cnn.com every 30 seconds for snippets starting with news and ends with a period.

    Reqquires (via pip install):
        aiven-client 2.10.0
        kafka-python 2.0.2
        psycopg2     2.8.6
        requests     2.25.1

    References:
        https://github.com/aiven/aiven-examples
        https://help.aiven.io/en/articles/489572-getting-started-with-aiven-for-apache-kafka
        https://kafka.apache.org/intro


**Sample Output:**

```
python AivenCloudTest.py --url https://www.cnn.com --regex "news.+\." --delay 30
inserting: 24/03/2021 01:25:01 https://www.cnn.com 200 [{"location": 5400, "text": "news and information on the...
inserting: 24/03/2021 01:25:02 https://www.cnn.com 200 [{"location": 5400, "text": "news and information on the...
inserting: 24/03/2021 01:25:03 https://www.cnn.com 200 [{"location": 5400, "text": "news and information on the...
inserting: 24/03/2021 01:25:04 https://www.cnn.com 200 [{"location": 5400, "text": "news and information on the...
```

![sample_output](https://user-images.githubusercontent.com/9458979/112328708-b32a1100-8d0a-11eb-923b-682676259e93.JPG)
