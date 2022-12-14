#!/usr/bin/env python

from app import app

app.config["DEBUG"] = True

if __name__=="__main__":
    app.run(host="0.0.0.0", port=4222)


