# A Comparative Analysis of Graph Signal Recovery Methods for Big Data Networks #

This repository provides the source code for my masters thesis "A Comparative 
Analysis of Graph Signal Recovery Methods for Big Data Networks" which can be
found [here](http://urn.fi/URN:NBN:fi:aalto-201710307413). The thesis analyses
different methods for graph signal recovery (also reffered to as graph semi-supervised learning or graph regression) on a variety of real and synthetic networks. In order to 
ensure a platform independent evaluation, all methods have been 
implemented in Scala over Spark/GraphX from scratch. More information regarding
the thesis objectives and results can be found in the `Presentaion.pdf` file. 

The code is maintained by Alexandru Mara (alexandru.mara(at)ugent.be).

#### Algorithms ####
The repository contains highly scalable implementations of the following algorithms:

* Average Consensus: the average of the node atributes is computed and assignd to each node in the graph
* Community-based Labelling: a community detection algorithm is used to partition the nodes and a majority vote is used to decide the label all nodes in each community will share
* Label Propagation: Zhou et. al. 2002
* Sparse Label Propagation: Jung et. al. 2016
* Network Lasso: Hallac et. al. 2015

## Instalation and Usage ##

The code requires no instalation. The provided `pom.xml` file contains the maven
dependencies of the project as well as the Spark and GraphX dependencies.

The simplest way to test the code is to import it in the Scala IDE for Eclipse
and use Maven to manage the project dependencies. 

The algorithms are located under `src/main/scala/algorithms`. The test files
to evaluate all the methods on each dataset can be located under 
`src/main/scala/testGraphs`. This folder contains one scala source file for each
real and each synthetic graph and will run all algorithms on the data. Some additional helper and graph preprocessing functions are also provided.

The output obtained after execution one algorithm consists of three folder and
one csv file. The csv file details the evolution of the normalize MSE for each iteration.
The remaining folders contain each a single text file. These text files consist of two
columns (nodeID, nodeSignalValue). One of the folders ('real_l') contains the real node
 labels, the second one ('samples') contains only the nodes selected as samples and their corresponding signals and lastly one folder ('_labels') contains the signal values 
predicted by the method selected. 

### Citing ###

Please consider giving the repo a star :) and citing the following papers if you find this code useful in your research:

@mastersthesis{Aaltodoc:http://urn.fi/URN:NBN:fi:aalto-201710307413,
  title={A Comparative Analysis of Graph Signal Recovery Methods for Big Data Networks},
  author={Mara, Alexandru},
  year={2017-10-23},
  language={en},
  pages={45+6},
  keyword={graph signal recovery; semi-supervised learning; benchmark; graphs},
  type={G2 Pro gradu, diplomityö},
  url={http://urn.fi/URN:NBN:fi:aalto-201710307413},
}

@INPROCEEDINGS{nlassoMaraJung, 
  author = {A.\ Mara and A.\ Jung}, 
  title = {Recovery Conditions and Sampling Strategies for Network Lasso},
  journal = {51st Asilomar Conference on Signals, Systems and Computers}, 
  year = {2017}
}

@article{jung2016semi,
  title={Semi-supervised learning via sparse label propagation},
  author={Jung, Alexander and Hero III, Alfred O and Mara, Alexandru and Jahromi, Saeed},
  journal={arXiv preprint arXiv:1612.01414},
  year={2016}
}

@inproceedings{hallac2015network,
  title={Network lasso: Clustering and optimization in large graphs},
  author={Hallac, David and Leskovec, Jure and Boyd, Stephen},
  booktitle={Proceedings of the 21th ACM SIGKDD international conference on knowledge discovery and data mining},
  pages={387--396},
  year={2015},
  organization={ACM}
}




