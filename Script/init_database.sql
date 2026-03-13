/* 
========================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWareHouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three seperate database: 'Bronze', 'Silver', and 'Gold'.
*/


# Data Base is Created Here.
DROP DATABASE IF EXISTS DataWareHouse;
CREATE DATABASE DataWareHouse;

# Created a sepertate DB for the Bronze, Silver and Gold Layers.
CREATE DATABASE dw_Bronze;
CREATE DATABASE dw_Silver;
CREATE DATABASE dw_Gold;
