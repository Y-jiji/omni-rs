# ignore binary files from dropbox
for f in $(ls)
do
    attr -r com.dropbox.ignore "$(pwd)/$f"
done
for f in $(cat ".gitignore")
do
    f=$(echo $f | sed -e 's/^\///g')
	echo "$f"
	# don't you dare ignore the root folder 🤬
	if [[ "$f" != "" ]]
	then
        attr -s com.dropbox.ignored -V 1 "$(pwd)/$f"
	fi
done
# ignore .git from dropbox (it only gets larger)
attr -s com.dropbox.ignored -V 1 "$(pwd)/.git"
