import pytumblr
import json
json_file_path = r"credenciais.json"

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
       print(f"{len(dashboard_posts['posts'])} foram obtidos!")
       for post in dashboard_posts['posts']:
        print('-')
        name = post['blog_name']
        print(f"Nome blog: {name}")
        type = post['type']
        print(f"Post Type: {type}")
        print(f"Post URL: {post['post_url']}")
        blog = post["blog"]
        avatar = blog["avatar"]
        for avatars in avatar:
          #PRIMEIRA URL É A MAIOR FOTO, talvez so baixar a primeira.
          print(avatars["url"])
    else:
      print("Could not retrieve dashboard posts. Check authentication and permissions.")
#salvar foto png!! e salvar os dados do json