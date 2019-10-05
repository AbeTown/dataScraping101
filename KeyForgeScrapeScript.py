'''made by yada'''

import json
import requests
import mysql.connector

num_deck_inserted = 1

def insert_card(connector, data):
	cursor = connector.cursor()
	insert_query = ("INSERT IGNORE INTO cards "
              "(id, card_title, house, card_type, front_image, card_text, traits, amber, power, armor, rarity, flavor_text, card_number, expansion, is_maverick)"
              "VALUES (%(id)s, %(card_title)s, %(house)s, %(card_type)s, %(front_image)s, %(card_text)s, %(traits)s, %(amber)s, %(power)s, %(armor)s, %(rarity)s, %(flavor_text)s, %(card_number)s, %(expansion)s, %(is_maverick)s)")
	cursor.execute(insert_query, data)
	connector.commit()
	cursor.close()

def insert_deck(connector, data):
	cursor = connector.cursor()
	insert_query = ("INSERT IGNORE INTO decks "
					"(id, name)"
					"VALUES (%(id)s, %(name)s)");
	cursor.execute(insert_query, data)
	connector.commit()
	cursor.close()

def insert_cards(connector, card_hashes, deck_id):
	cursor = connector.cursor()
	card_hash_dict = {}
	for card_hash in card_hashes:
		if card_hash in card_hash_dict:
			card_hash_dict[card_hash] = card_hash_dict[card_hash] + 1
		else:
			card_hash_dict[card_hash] = 1

	insert_query = ("INSERT IGNORE into DeckCards "
					"(card_id, deck_id, num_copies) "
					"VALUES (%(card_id)s, %(deck_id)s, %(num_copies)s)")

	for card_hash, count in card_hash_dict.iteritems():
		data = {
			'card_id': card_hash,
			'deck_id': deck_id,
			'num_copies': count
		}
		cursor.execute(insert_query, data)
	connector.commit()
	cursor.close()

cnx = mysql.connector.connect(user='root', database='keyforge')

for page in range(1, 160000/20):
	print("processing page " + str(page))
	print(num_deck_inserted)
	url = "https://www.keyforgegame.com/api/decks/?page=" + str(page) + "&page_size=20&ordering=-date&links=cards"

	result = requests.get(url).json()

	cards_list = result[u'_linked'][u'cards']

	for card in cards_list:
		card_data = {
			'id': card[u'id'],
			'card_title': card[u'card_title'],
			'house': card[u'house'],
			'card_type': card[u'card_type'],
			'front_image': card[u'front_image'],
			'card_text': card[u'card_text'],
			'traits': card[u'traits'],
			'amber': int(card[u'amber']),
			'power': int(card[u'power']),
			'armor': int(card[u'armor']),
			'rarity': card[u'rarity'],
			'flavor_text': card[u'flavor_text'],
			'card_number': card[u'card_number'],
			'expansion': card[u'expansion'],
			'is_maverick': card[u'is_maverick']
		}
		insert_card(cnx, card_data)

	decks_list = result[u'data']
	for deck in decks_list:
		deck_data = {
			'id': deck[u'id'],
			'name': deck[u'name']
		}
		num_deck_inserted = num_deck_inserted + 1
		insert_deck(cnx, deck_data)
		insert_cards(cnx, deck[u'_links'][u'cards'], deck[u'id'])


'''
CREATE TABLE Cards (
	id VARCHAR(60) PRIMARY KEY,
	card_title VARCHAR(100),
	card_type VARCHAR(30),
	house VARCHAR(15),
	front_image VARCHAR(100),
	card_text VARCHAR(300),
	traits VARCHAR(100),
	amber INT UNSIGNED,
	power INT UNSIGNED,
	armor INT UNSIGNED,
	rarity VARCHAR(20),
	flavor_text VARCHAR(500),
	card_number INT UNSIGNED,
	expansion INT UNSIGNED,
	is_maverick BOOLEAN
);

CREATE TABLE Decks (
	id VARCHAR(60),
	name VARCHAR(30)
);

CREATE TABLE DeckCards (
	card_id VARCHAR(60),
	deck_id VARCHAR(60),
	num_copies INT UNSIGNED,
	INDEX cards(card_id),
	INDEX decks(deck_id)
);
'''

'''
sample API output

{
   "id":"5a1ee413-4b39-467f-a0bf-e5935f1edf9b",
   "card_title":"Dr. Escotera",
   "house":"Logos",
   "card_type":"Creature",
   "front_image":"https://cdn.keyforgegame.com/media/card_front/en/341_140_GGR9WQGX52CC_en.png",
   "card_text":"Play: Gain 1<A> for each forged key your opponent has.",
   "traits":"Cyborg Scientist",
   "amber":0,
   "power":4,
   "armor":0,
   "rarity":"Common",
   "flavor_text":"Interesting reaction, but what does it mean?",
   "card_number":140,
   "expansion":341,
   "is_maverick":false
}
'''
