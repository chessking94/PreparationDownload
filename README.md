# PreparationDownload

This project downloads all games of selected users in the Chess.com and Lichess online archives, merge the files, sort the games by date, and clean for common issues.

I utilize the free software pgn-extract in order to clean game files. This software and its documentation can be found at https://www.cs.kent.ac.uk/people/staff/djb/pgn-extract/; I take no credit for its use.

The path of the user input file is hard-coded and will need to be changed for anyone else to repurpose.

I converted the script into something compatible with the command line. After a while setting parameters manually in the script got a little old.