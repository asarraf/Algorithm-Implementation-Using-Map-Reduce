Algorithm-Implementation-Using-Map-Reduce
=========================================

Realized 3 popular applications of Map Reduce in this pet project
1) Implemented Page Rank algorithm to estimate the page rank of all nodes given a unidirectional connected graph represented in a form of adjacency matrix as input
2) Constructed of Inverted Index for all the words occurring in a given set of documents
3) Calculated Matrix (Vector) Product of two 3 X 3 matrices

PAGE RANK ALGORITHM: Project to find the page rank of nodes in the a unidirectional connected graph.
1. A file containing the Adjacency Matrix for the graph was fetched from the Hadoop Distributed File System (HDFS) as an input to the Map Reduce task.
2. The output was the final page rank (in form of probability of reaching the Node) was stored in back to a file on Hadoop Distributed File System (HDFS).

INVERTED INDEX: Project to read documents as input and construct an inverted index for each word occurring in those documents.
1. Number of Documents containing text related to various countries were fetched from the Hadoop Distributed File System (HDFS) as an input for the Map Reduce task.
2. The output representing an inverted index (with key as the Words and Value as names of documents in which this word appears) was stored in back to a file on Hadoop Distributed File System (HDFS).

MATRIX MULTIPLICATION: Project to Read a file containing two 3 X 3 Matrices and calculate their Vector Product.
1. A file containing two Matrices - MatrixA and MatrixB, was fetched from the Hadoop Distributed File System (HDFS) as an input for the Map Reduce task.
2. The output was the Vector Product (MatrixC = MatrixA X MatrixB) that was stored back to a file on Hadoop Distributed File System (HDFS).
