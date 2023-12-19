#!/bin/bash

# Change to the directory containing the "final-data" folder
cd data/final-stats


# # Loop over m-*, w-* folders and the 'all' folder
# for dir in m-* w-* all
# do
#     # Check if directory exists
#     if [ -d "$dir" ]; then
#         # Navigate into each directory
#         cd "$dir"

#         # Delete the s6 folders if they exist
#         if [ -d "s6" ]; then
#             rm -rf s6
#         fi

#         # Rename the folders in reverse order
#         for (( i=5; i>=1; i-- )); do
#             if [ -d "s$i" ]; then
#                 mv "s$i" "s$((i+1))"
#             fi
#         done

#         # Navigate back to the main directory
#         cd ..
#     fi
# done

# Loop over m-*, w-* folders and the 'all' folder
for dir in m-* w-* all
do
    # Check if directory exists
    if [ -d "$dir" ]; then
        # Navigate into each directory
        cd "$dir"

        # Check if there are s* subdirectories
        if compgen -G "s*" > /dev/null; then

            # Loop over s* subdirectories
            for subdir in s*
            do
                # Navigate into the subdirectory
                cd "$subdir"

                # Check if the file exists and rename it
                if [ -f "part-r-00000" ]; then
                    mv part-r-00000 data.json
                fi

                # Navigate back to the parent directory
                cd ..

            done
        fi

        # Navigate back to the main directory
        cd ..
    fi
done
