import os

import pytumblr
import json
import requests

json_file_path = r"credenciais.json"
imagens = "tumblr_avatars"
posts = "posts_data.json"

if not os.path.exists(imagens):
  os.makedirs(imagens)

with open(json_file_path, "r") as f:
    credentials = json.load(f)
    client = pytumblr.TumblrRestClient(
      credentials["consumer_key"],
      credentials["consumer_secret"],
      credentials["oath_token"],
      credentials["oath_secret"]
    )
    dashboard_posts = client.dashboard()
    if 'posts' in dashboard_posts:
      posts_lista = dashboard_posts['posts']
      print(f"{len(dashboard_posts['posts'])} foram obtidos!")
       
      for post in dashboard_posts['posts']:
        print('-')
        name = post['blog_name']
        try:
          avatar_url = post["blog"]["avatar"][0]["url"]
          img = requests.get(avatar_url, stream=True).content
          arq = f"{name}_avatar.png"
          caminho = os.path.join(imagens, arq)
          with open(caminho, 'wb') as handle:
            handle.write(img)
        except (KeyError, IndexError, Exception) as e:
          print(f"não baixou de {name}: {e}")
          
      with open(posts, 'w', encoding='utf-8') as f:
        json.dump(posts_lista, f, indent=4, ensure_ascii=False)
        
      print(f"Dados dos posts salvos em {posts}")
      
    else:
      print("Erro de autentificação")
        