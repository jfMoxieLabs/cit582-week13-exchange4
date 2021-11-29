from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)

@app.before_request
def create_session():
    g.session = scoped_session(DBSession)

@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """

def check_sig(payload,sig):
    pass

def fill_order(order,txes=[]):
    pass
  
def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    pass

""" End of helper methods """



@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        content = request.get_json(silent=True)
        print( f"content = {json.dumps(content)}" )
        columns = [ "sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform" ]
        fields = [ "sig", "payload" ]

        for field in fields:
            if not field in content.keys():
                print( f"{field} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
        
        for column in columns:
            if not column in content['payload'].keys():
                print( f"{column} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
            
        #Your code here
        #Note that you can access the database session using g.session

        # TODO: Check the signature
    payload = json.dumps(content['payload'])
    ret = False
    #Check if signature is valid
    # TODO: Add the order to the database

    if(content['payload']['platform'] == "Algorand"):
      print('algorand found')
      algo_pk = content['payload']['sender_pk']
      algo_sig_str = content['sig']
      if algosdk.util.verify_bytes(payload.encode('utf-8'),algo_sig_str,algo_pk):
        #Create the order
        new_order = Order(signature = algo_sig_str, receiver_pk  = content['payload']['receiver_pk'], sender_pk = content['payload']['sender_pk'], buy_currency = content['payload']['buy_currency'], sell_currency = content['payload']['sell_currency'], buy_amount = content['payload']['buy_amount'], sell_amount = content['payload']['sell_amount'])
        g.session.add(new_order)
        #g.session.commit()
        ret = True
        

      else:
        new_log = Log(message = json.dumps(payload))
        g.session.add(new_log)
        #g.session.commit()
        #return jsonify( False )
        ret = False


    elif (content['payload']['platform'] == "Ethereum"):
      print("eth found")
      eth_pk = content['payload']['sender_pk']
      eth_sig_obj = content['sig']
      eth_encoded_msg = eth_account.messages.encode_defunct(text=payload)
      if eth_account.Account.recover_message(eth_encoded_msg,signature=eth_sig_obj) == eth_pk:
        new_order = Order(signature = eth_sig_obj, receiver_pk  = content['payload']['receiver_pk'], sender_pk = content['payload']['sender_pk'], buy_currency = content['payload']['buy_currency'], sell_currency = content['payload']['sell_currency'], buy_amount = content['payload']['buy_amount'], sell_amount = content['payload']['sell_amount'])
        g.session.add(new_order)
        #g.session.commit()
        ret = True

      else:
        new_log = Log(message = json.dumps(payload))
        g.session.add(new_log)
        #g.session.commit()
        #return jsonify( False )
        ret = False
        
    else:
        new_log = Log(message = json.dumps(payload))
        g.session.add(new_log)
        #g.session.commit()
        #return jsonify( False )
        ret = False

    
        
        
        # TODO: Fill the order
    unfulfilled_orders = g.session.query(Order).filter(Order.filled == None).all()
    orders_length = len(unfulfilled_orders)
    if (orders_length >=1 ):
        for unfulfilled_order in unfulfilled_orders:
        #Find Unfilled order that matches
            if unfulfilled_order.sell_currency == new_order.buy_currency and unfulfilled_order.buy_currency == new_order.sell_currency and (unfulfilled_order.sell_amount / unfulfilled_order.buy_amount >= new_order.buy_amount / new_order.sell_amount):
                if unfulfilled_order.buy_amount == new_order.sell_amount and unfulfilled_order.sell_amount == new_order.buy_amount:
                    timestamp = datetime.now()
                    unfulfilled_order.filled = timestamp
                    new_order.filled = timestamp
                    unfulfilled_order.counterparty_id = new_order.id
                    new_order.counterparty_id = unfulfilled_order.id
                    break
                elif(unfulfilled_order.buy_amount > new_order.sell_amount):
                    timestamp = datetime.now()
                    unfulfilled_order.filled = timestamp
                    new_order.filled = timestamp
                    unfulfilled_order.counterparty_id = new_order.id
                    new_order.counterparty_id = unfulfilled_order.id
                    exchange_rate = unfulfilled_order.buy_amount / unfulfilled_order.sell_amount
                    child_buy_amount = unfulfilled_order.buy_amount - new_order.sell_amount
                    new_child = Order(receiver_pk  = unfulfilled_order.receiver_pk, sender_pk = unfulfilled_order.sender_pk, buy_currency = unfulfilled_order.buy_currency, sell_currency = unfulfilled_order.sell_currency, buy_amount = child_buy_amount, sell_amount = (child_buy_amount / exchange_rate), creator_id = unfulfilled_order.id)
                    g.session.add(new_child)
                    break
                elif(unfulfilled_order.sell_amount < new_order.buy_amount):
                    timestamp = datetime.now()
                    unfulfilled_order.filled = timestamp
                    new_order.filled = timestamp
                    unfulfilled_order.counterparty_id = new_order.id
                    new_order.counterparty_id = unfulfilled_order.id
                    exchange_rate = new_order.buy_amount / new_order.sell_amount
                    child_buy_amount = new_order.buy_amount - unfulfilled_order.sell_amount
                    new_child = Order(receiver_pk  = new_order.receiver_pk, sender_pk = new_order.sender_pk, buy_currency = new_order.buy_currency, sell_currency = new_order.sell_currency, buy_amount = child_buy_amount, sell_amount = (child_buy_amount / exchange_rate), creator_id = new_order.id)
                    g.session.add(new_child)
                    break
                
    
        
        # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful
    g.session.commit()
    return jsonify( ret )  

@app.route('/order_book')
def order_book():
    #Your code here
    #Note that you can access the database session using g.session
    key = 'data'
    result = {}
    result.setdefault(key, [])
    orders = g.session.query(Order).all()
    orders_length = len(orders)
    print("In order Book")
    print('order length = ', orders_length)
    for order in orders:
        result[key].append({'signature': order.signature, 'receiver_pk': order.receiver_pk, 'sender_pk': order.sender_pk, 'buy_currency': order.buy_currency, 'sell_currency': order.sell_currency, 'buy_amount': order.buy_amount, 'sell_amount': order.sell_amount})
        print(result)

    return jsonify(result)

if __name__ == '__main__':
    app.run(port='5002')