#!/usr/bin/env python
# encoding: utf-8

""""
Computes the Pearson Correlation/ Cosine-Similarity for users.
It then recommends movies based on Memory Based Collaborative Filtering for k most similar users.

Reference: http://en.wikipedia.org/wiki/Collaborative_filtering
Data Set: http://www.grouplens.org/node/73  (MovieLens 10M data)

Caution: May take a while to execute for certain input values. Fetch Coffee in the meanwhile :)

Created by Ritwik Yadav on 2013-01-29.
Copyright (c) 2013 IIT Kharagpur. All rights reserved.
"""

import sys
import os
from pymongo import MongoClient
import math

# This function creates the ratings table. To be executed once!
def create_rating_db():
	f=open('ratings.dat')
	connection=MongoClient()
	ratings=connection.movielens.ratings
	line=f.readline()
	while line!='':
		try:
			record=line.strip('\r\n').split('::')[0:3]
			rating={}
			rating['user']=int(record[0])
			rating['movie']=int(record[1])
			rating['rating']=float(record[2])
			ratings.insert(rating)
			print rating
			del rating
			del record
			line=f.readline()
		except:
			print 'Error', repr(line)

# This function computes the average of all the users and stores it in the database.
# To be executed once!
def create_average_db():
	average_list=[]
	#71567 is the number of users for which data is available.
	for i in range(71567):
		average_list.append([0.0,0])
	connection=MongoClient()
	ratings=connection.movielens.ratings
	averages=connection.movielens.averages
	for rating in ratings.find():
		index=int(rating['user']-1)
		average_list[index][0]*=average_list[index][1]
		average_list[index][0]+=rating['rating']
		average_list[index][1]+=1
		average_list[index][0]/=average_list[index][1]
		del index
	#71567 is the number of users for which data is available.
	for i in range(71567):
		averages.insert({'user': i+1, 'average': average_list[i][0] })

# This function creates the table associating movie id with movie name.
def create_movie_name_db():
	f=open('movies.dat')
	connection=MongoClient()
	movies=connection.movielens.movies
	line=f.readline()
	while line!='':
		try:
			record=line.strip('\r\n').split('::')[0:2]
			movie={}
			movie['id']=int(record[0])
			movie['name']=repr(record[1])
			movies.insert(movie)
			print movie
			del movie
			del record
			line=f.readline()
		except:
			print 'Error', repr(line)

# This function computes the Pearson and cosine based similarity between the given user
# and all other users. It then returns the top k most similar users.
def corr_similarity(user, k, choice):
	user_terms=[]
	for i in range(71567):
		user_terms.append([0,0,0,0,0,0])
	connection=MongoClient()
	averages=connection.movielens.averages
	ratings=connection.movielens.ratings
	user_avg=averages.find_one({'user': user})['average']
	user_ratings=ratings.find({'user': user})
	for rating in user_ratings:
		movie=rating['movie']
		other_ratings_for_movie=ratings.find({'movie': movie, 'user': {'$ne': user}})
		for other_rating in other_ratings_for_movie:
			index=int(other_rating['user']-1)
			other_user_avg=averages.find_one({'user': user})['average']
			user_terms[index][0]+=(rating['rating']-user_avg)*(other_rating['rating']-other_user_avg)
			user_terms[index][1]+=(rating['rating']-user_avg)*(rating['rating']-user_avg)
			user_terms[index][2]+=(other_rating['rating']-other_user_avg)*(other_rating['rating']-other_user_avg)
			user_terms[index][3]+=(rating['rating'])*(other_rating['rating'])
			user_terms[index][4]+=(rating['rating'])*(rating['rating'])
			user_terms[index][5]+=(other_rating['rating'])*(other_rating['rating'])
			del index
			del other_user_avg
		del movie
	user_corr_pearson=[]
	user_corr_cosine=[]
	for i in range(71567):
		try:
			user_corr_pearson.append((i+1, user_terms[i][0]/math.sqrt(user_terms[i][1]*user_terms[i][2])))
			user_corr_cosine.append((i+1, user_terms[i][3]/math.sqrt(user_terms[i][4]*user_terms[i][5])))
		except ZeroDivisionError:
			pass
	if choice ==1:
		user_corr_pearson.sort(key=lambda x: x[1])
		return user_corr_pearson[-k:]
	else:
		user_corr_cosine.sort(key=lambda x: x[1])
		return user_corr_cosine[-k:]

#This function pools all the movies which are rated by the k most similar users but
#not seen by the active user. The aggregate ratings for these movies is computed.
#The top 5 results with maximum aggregate scores are displayed.
def compute_recommendations(user, k, choice):
	user_corr=corr_similarity(user, k, choice)
	connection=MongoClient()
	ratings=connection.movielens.ratings
	movies=connection.movielens.movies
	movieset=set([])
	recommendations=[]
	for near_user in user_corr:
		newmovies=ratings.find({'user': near_user[0]}, {'movie': 1})
		newmovies=[record['movie'] for record in newmovies]
		movieset=movieset.union(set(newmovies))
		del newmovies
	oldmovies=ratings.find({'user': user}, {'movie': 1})
	oldmovies=[record['movie'] for record in oldmovies]
	movieset=movieset.difference(set(oldmovies))
	for newmovie in movieset:
		prob_rating=0.0
		k=0.0
		for near_user in user_corr:
			cursor=ratings.find_one({'user': near_user[0], 'movie': newmovie})
			if cursor==None:
				pass
			else:
				prob_rating+=near_user[1]*cursor['rating']
				k+=math.fabs(near_user[1])
			del cursor
		recommendations.append((newmovie,prob_rating/k))
		del prob_rating
		del k
	recommendations.sort(key=lambda x: x[1], reverse=True)
	oldmovies=ratings.find({'user': user}, {'movie': 1, 'rating':1})
	print 'Movie Ratings of the User:', user
	print '----------------------------------------\n'
	for old_movie in oldmovies:
		print (movies.find_one({'id': old_movie['movie']})['name']), old_movie['rating']
 	print '============================================================================\n'
	print 'Top 5 (or less) Movie Recommendations:'
	print '----------------------------------------\n'
	for i in range(5):
		print (movies.find_one({'id': recommendations[i][0]})['name'])
	print '============================================================================\n'
					
				
if __name__ == '__main__':
	print 'Enter User ID (from 1 to 71567):'
	user=int(raw_input())
	print 'Enter Value of k (nearest neighbours): '
	k=int(raw_input())
	print 'Enter 1 for Pearson Correlation or 2 for Cosine-Similarity:'
	choice=int(raw_input())
 	compute_recommendations(user, k, choice)

