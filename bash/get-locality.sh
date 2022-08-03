PATHFILE=$1

while IFS= read -r line
do
	filepath=$(dirname "${line}")
	filename=$(basename "${line}")
	repl=$(echo "${filepath}/'.(get)(filerepl)(locality)'")
	repld=${repl/filerepl/${filename}}
	echo "$(cat ${repld})"
done < ${PATHFILE}
