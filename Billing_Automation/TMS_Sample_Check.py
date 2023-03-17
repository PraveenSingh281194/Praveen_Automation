import requests

url = "https://dev.api.corp.local/aplnavigator/v1/opentaxlot/AJ6Y55"

payload={}
headers = {
  'Authorization': 'Bearer eyJraWQiOiJBaXAwVTlERmlvSE9VQ0ZqNG41VFJtQkx0d3RIdi1DcVZKcW9PNjJoeUVjIiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULnJhMDZ4eDIxOTJFWVB2cS00cndOZ193MlRicjJxdTRYbXM5OTRpUnM4SlkiLCJpc3MiOiJodHRwczovL2Fzc2V0bWFyay5va3RhcHJldmlldy5jb20vb2F1dGgyL2RlZmF1bHQiLCJhdWQiOiJhcGk6Ly9kZWZhdWx0IiwiaWF0IjoxNjc0MjE3MDQ2LCJleHAiOjE2NzQyMjA2NDYsImNpZCI6IjBvYXFtdTkzODR3bEp6WVJIMGg3Iiwic2NwIjpbIlNTT0dVSUQiXSwic3ViIjoiMG9hcW11OTM4NHdsSnpZUkgwaDcifQ.SXu9M5-UoEKrBSo12d10kABTae7nqFcIWNwJLf_G8qFDIJV31_P5E_WRfGD8hVbTuekxqnTj_Fl1OBgSeV6Abjcy5dkepNhP1LhAFRRG-rIcPLZ0ZDK6ujlqd40KuYOlfmxPopdd7Wx5uB6EkbidccQEKU7YoE4MX1QY8CZemT2ezOpDT0uVcQf4ymhmcPYKlPNQIFUUoOc2zqsEUchzyL4Dj-zP2Bbr66M7hmbHxGIaFV5slg6PVVg6VUQw7YH61SgrMynWlxUIM6n-x3Dp9lyxLXENl3rMdfLxQtKXGln6VvPbnFHuVOagJj-dofH53W7xAvHEs1jPuLBU7riwEw',
  'Ocp-Apim-Subscription-Key': '154f340d85174884967880d3317af8db',
  'Content-Type': 'text/xml; charset=utf-8'
 
}

response = requests.request("GET", url, headers=headers, data=payload,verify=False)

print(response.text)
