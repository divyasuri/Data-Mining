from pyspark import SparkContext
import datetime
import sys 

def AdjacencyMatrix(nodes,dictEdges): #compute adjacency matrix for original graph
    dicAij = {}
    for i in nodes:
        for j in nodes:
            if j in dictEdges[i]:
                A = 1 
            else:
                A = 0 
            dicAij[(i,j)] = A
    return dicAij

def ModCalc(community,nodeDegrees,dicAij,m): #calculate component for mod calculations for individual clusters
    y = 0
    for i in community:
        for j in community:
            add = dicAij[(i, j)] - ((nodeDegrees[i] * nodeDegrees[j]) / (2*m))
            y += add
    return y

def FindCommunities(dictEdges): #find communities with size greater than 1 
    visited = set()
    communities = []
    for node in dictEdges:
        if node not in visited:
            community = FindSingleCommunity(node, dictEdges, visited)
            communities.append(community)
    return communities

def FindSingleCommunity(root, dictEdges, visited): #find individual communities
    community = [root]
    visited.add(root)
    queue = []
    queue.append(root)
    while queue:
        node = queue.pop(0)
        for child in dictEdges[node]:
            if child not in visited:
                queue.append(child)
                community.append(child)
                visited.add(child)
    return community

def RemoveEdge(edge_to_cut, dictEdges): #remove edge from original graph that corresponds to highest betweeness
    dictEdges[edge_to_cut[0][0]].remove(edge_to_cut[0][1])
    dictEdges[edge_to_cut[0][1]].remove(edge_to_cut[0][0])
    return dictEdges

def Betweenness(root,dictEdges): #function to calculate betweenness 
    explored = []
    nodeLevels = {}
    nodeLevels[root] = 0 
    queue = []
    queue.append(root)
    childParent = {}
    parentChild = {}
    while queue:
        node = queue.pop(0)
        if node not in explored:
            explored.append(node)
            children = dictEdges[node]
            for child in children:
                queue.append(child)
                if child not in explored:
                    if child not in childParent:
                        childParent[child] = []
                        childParent[child].append(node)
                    else:
                        childParent[child].append(node)
                    if child not in nodeLevels:
                        nodeLevels[child] = nodeLevels[node] + 1
                    if node not in parentChild:
                        parentChild[node] = []
                        parentChild[node].append(child)
                    else:
                        parentChild[node].append(child)
                    if child not in parentChild[node]:
                        parentChild[node].append(child)
    nodeCredits = {}
    edgeLabels = {}
    i = len(explored) - 1
    while i > 0:
        node_to_check = explored[i]
        if node_to_check not in parentChild:
            nodeCredits[node_to_check] = 1
            parents = childParent[node_to_check]
            size = len(parents)
            for par in parents:
                if par not in nodeCredits:
                    nodeCredits[par] = 1
                int_edge = float(nodeCredits[node_to_check]) / size
                nodeCredits[par] = nodeCredits[par] + int_edge
                edge_list = [node_to_check,par]
                edge_list1 = sorted(edge_list)
                edge_list2 = sorted(edge_list,reverse=True)
                edge = (tuple(edge_list1),tuple(edge_list2))
                edgeLabels[edge] = int_edge / 2
        elif node_to_check not in nodeCredits:
            nodeCredits[node_to_check] = 1
            parents = childParent[node_to_check]
            size = len(parents)
            for par in parents:
                if par not in nodeCredits:
                    nodeCredits[par] = 1
                int_edge = float(nodeCredits[node_to_check]) / size
                nodeCredits[par] = nodeCredits[par] + int_edge
                edge_list = [node_to_check,par]
                edge_list1 = sorted(edge_list)
                edge_list2 = sorted(edge_list,reverse=True)
                edge = (tuple(edge_list1),tuple(edge_list2))
                edgeLabels[edge] = int_edge / 2
        elif node_to_check in nodeCredits:
            parents = childParent[node_to_check]
            size = len(parents)
            for par in parents:
                if par not in nodeCredits:
                    nodeCredits[par] = 1
                int_edge = float(nodeCredits[node_to_check]) / size
                nodeCredits[par] = nodeCredits[par] + int_edge
                edge_list = [node_to_check,par]
                edge_list1 = sorted(edge_list)
                edge_list2 = sorted(edge_list,reverse=True)
                edge = (tuple(edge_list1),tuple(edge_list2))
                edgeLabels[edge] = int_edge / 2
        i = i - 1
    final_edges = []
    for k,v in edgeLabels.items():
        inp = (k[0],v)
        final_edges.append(inp)
    return final_edges

def main(): 
	data = gn.textFile(input_file)
	data_amend = data.map(lambda x:x.split(" "))
	edges = data_amend.map(lambda x: (x[0],x[1])).flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda x, y: x + y)
	edges_collected = edges.collect()
	dictEdges = {}
	for edge in edges_collected:
	    if edge[0] not in dictEdges:
	        dictEdges[edge[0]] = edge[1]
	edge_betweenness= edges.flatMap(lambda x: Betweenness(x[0],dictEdges)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0][1]).sortBy(lambda x: x[0][0])
	betweenness_collected = edge_betweenness.collect()
	with open(bet_outfile,'w') as outfile1:
		for bet in betweenness_collected:
			outfile1.write(str(bet[0]))
			outfile1.write(", ")
			outfile1.write(str(bet[1]))
			outfile1.write('\n')
	outfile1.close()
	m = data_amend.count()
	normalization_m = 1/ (2*m)
	nodeDegrees = {}
	for k,v in dictEdges.items():
		nodeDegrees[k] = len(v)
	nodes = list(nodeDegrees.keys())
	dicAij = AdjacencyMatrix(nodes,dictEdges)
	null_modularity = 0 #calculate null modularity
	for k,v in dicAij.items():
		null_modularity = null_modularity + (v - ((nodeDegrees[k[0]]*nodeDegrees[k[1]]) / (2*m)))
	null_modularity = null_modularity * normalization_m 
	dicModu = {}
	edges_int = data_amend.map(lambda x: (x[0],x[1])).collect()
	dicModu[null_modularity] = FindCommunities(dictEdges) 
	while len(edges_int) > 0: #keep iterating until all edges have been removed
		highest_betweenness = edge_betweenness.sortBy(lambda x: x[1],False).map(lambda x: x[0]).take(1)
		dictEdges = RemoveEdge(highest_betweenness, dictEdges)
		communities = FindCommunities(dictEdges)
		edge_betweenness= edges.flatMap(lambda x: Betweenness(x[0],dictEdges)).reduceByKey(lambda x, y: x + y)
		new_mod = 0
		for community in communities: #aggregate mods for individual communities to get mod for partitioning of graph
			new_mod += ModCalc(community,nodeDegrees,dicAij,m)
		new_mod = new_mod / normalization_m 
		dicModu[new_mod] = communities
		high_bet = tuple(highest_betweenness[0])
		for e in edges_int:
			if e == (high_bet[0],high_bet[1]) or e == (high_bet[1],high_bet[0]):
				edges_int.remove(e)
	max_mod = max(list(dicModu.keys())) #find community with high mod value
	max_communities = dicModu[max_mod]
	com_list = []
	for com in max_communities:
		com_list.append(sorted(com))
	maxLength = 0 
	for com in com_list:
		if len(com) >= maxLength:
			maxLength = len(com)
	community_length = {} #partition data by len of community
	for i in range(1,maxLength + 1):
		community_length[i] = []
	for com in com_list:
		for k in community_length.keys():
			if len(com) == k:
				community_length[k].append(com)
	final = []
	for k2,v2 in community_length.items():
		final.append(sorted(v2))
	with open(com_outfile,'w') as outfile2: #write out into output file
		for lst in final:
			for x in lst:
				if len(x) == 1:
					outfile2.write("'")
					outfile2.write(x[0])
					outfile2.write("'")
					outfile2.write('\n')
				else:
					for i in x:
						outfile2.write("'")
						outfile2.write(i)
						outfile2.write("'")
						if i != x[-1]:
							outfile2.write(', ')
					outfile2.write('\n')
	outfile2.close()
	end_time = datetime.datetime.now()
	difference1 = end_time - start_time
	difference_duration1 = round(difference1.total_seconds(),2)
	print('Duration: ' + str(difference_duration1) +' seconds')

if __name__ == "__main__":
	start_time = datetime.datetime.now()
	input_file = sys.argv[1]
	bet_outfile = sys.argv[2]
	com_outfile = sys.argv[3]
	gn = SparkContext.getOrCreate()
	main()
