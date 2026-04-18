# app.py

# Existing imports

# Other existing code...

# Updated Payment object creation at line 407
payment = Payment(amount=sale.amount, payment_method=payment_method, ...)  # include the payment_method parameter

# Updated cash() function from lines 564-601

def cash():
    if sale:
        date_key = sale.created_at.date()  # determine date_key from sale.created_at
        if payment_type == 'долг' and sale.created_at.date() == p.created_at.date():
            # treat as 'sale' section
            # existing logic for sale
        else:
            # treat as 'debt' section
            # existing logic for debt
    # existing code...