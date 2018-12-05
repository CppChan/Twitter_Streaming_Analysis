# Twitter_Streaming_Analysis


### Reservoir Sampling Algorithm for tweets current hot tags

* Store all the first s elements of the stream to S.Suppose we have seen n-1 elements, and now the nth element arrives (n > s)
.With probability s/n, keep the nth element, else discard it. If we picked the nth element, then it replaces one of the s elements in the sample S, picked uniformly at random

* Statistic the hottest 5 tags and the average
length of the tweets in the list. And print them out.




### Bloom Filtering for detecting previously appeared tweets hot tag

* Suppose element y appears in the stream, and we want to know if we have seen y before. Compute h(y) for each hash function y
. If all the resulting bit positions are 1, say we have seen y before (false positive). If at least one of these positions is 0, say we have not seen y before (false negative).

