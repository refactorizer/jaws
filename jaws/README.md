
JAWS is a 'micro-framework' providing some convenience classes to avoid boilerplate in AWS client code. 

```
AwsS3Template s3 = new AwsS3Template();

s3.gzipWrite("example.gzip", "some file contents");

String contents = s3.gzipRead("example.gzip");
```