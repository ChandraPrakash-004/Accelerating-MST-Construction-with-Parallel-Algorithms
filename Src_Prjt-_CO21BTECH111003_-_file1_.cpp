#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <climits>
#include <chrono>

using namespace std;
using namespace std::chrono;

struct Edge {
    int src, dest, weight;
};

struct Subset {
    int parent, rank;
};

mutex mtx;

int find(vector<Subset>& subsets, int i) {
    if (subsets[i].parent != i)
        subsets[i].parent = find(subsets, subsets[i].parent);
    return subsets[i].parent;
}

void Union(vector<Subset>& subsets, int x, int y) {
    int xroot = find(subsets, x);
    int yroot = find(subsets, y);

    if (subsets[xroot].rank < subsets[yroot].rank)
        subsets[xroot].parent = yroot;
    else if (subsets[xroot].rank > subsets[yroot].rank)
        subsets[yroot].parent = xroot;
    else {
        subsets[yroot].parent = xroot;
        subsets[xroot].rank++;
    }
}

void BoruvkaMST(vector<Edge>& edges, int V, int numThreads, ofstream& outputFile) {
    vector<Edge> result(V - 1); // To store the MST edges
    vector<Subset> subsets(V);

    for (int v = 0; v < V; ++v) {
        subsets[v].parent = v;
        subsets[v].rank = 0;
    }

    int numTrees = V;
    int MSTweight = 0;
    int edgeCount = 0;

    while (numTrees > 1) {
        vector<Edge> closestEdge(V, { -1, -1, INT_MAX - 1 });
        vector<thread> threads;

        for (int t = 0; t < numThreads; ++t) {
            threads.emplace_back([t, &edges, &subsets, &closestEdge, V, numThreads] {
                for (int i = t; i < V; i += numThreads) {
                    for (auto& edge : edges) {
                        int set1 = find(subsets, edge.src);
                        int set2 = find(subsets, edge.dest);

                        if (set1 != set2) {
                            lock_guard<mutex> lock(mtx);
                            if (edge.weight < closestEdge[set1].weight)
                                closestEdge[set1] = edge;

                            if (edge.weight < closestEdge[set2].weight)
                                closestEdge[set2] = edge;
                        }
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        for (int i = 0; i < V; ++i) {
            if (closestEdge[i].weight != INT_MAX - 1) {
                int set1 = find(subsets, closestEdge[i].src);
                int set2 = find(subsets, closestEdge[i].dest);

                if (set1 != set2) {
                    result[edgeCount++] = closestEdge[i];
                    MSTweight += closestEdge[i].weight;
                    Union(subsets, set1, set2);
                    numTrees--;
                }
            }
        }
    }

    outputFile << "Edges in MST:" << endl;
    for (int i = 0; i < V - 1; ++i) {
        outputFile << "Edge " << result[i].src << " - " << result[i].dest << " with weight " << result[i].weight << endl;
    }
    outputFile << "Total weight of MST: " << MSTweight << endl;
}

int main() {
    ifstream inputFile("graph_input.txt"); // Read input file
    if (!inputFile.is_open()) {
        cerr << "Unable to open the file." << endl;
        return 1;
    }

    int V, E, numThreads;

    inputFile >> V >> E >> numThreads;
    if (V <= 0 || E <= 0 || numThreads <= 0) {
        cerr << "Invalid input: Number of vertices, edges, and threads must be positive." << endl;
        return 1;
    }

    vector<Edge> edges(E);
    for (int i = 0; i < E; ++i) {
        inputFile >> edges[i].src >> edges[i].dest >> edges[i].weight;
    }
    inputFile.close();

    ofstream outputFile("output.txt");
    if (!outputFile.is_open()) {
        cerr << "Unable to open the output file." << endl;
        return 1;
    }

    auto start = high_resolution_clock::now(); // Start measuring execution time

    // Finding MST
    BoruvkaMST(edges, V, numThreads, outputFile);

    auto stop = high_resolution_clock::now(); // Stop measuring execution time
    auto duration = duration_cast<milliseconds>(stop - start);

    outputFile << "Execution Time: " << duration.count() << " milliseconds" << endl;

    outputFile.close();

    return 0;
}

