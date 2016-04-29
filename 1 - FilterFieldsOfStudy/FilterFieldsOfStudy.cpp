#include <bits/stdc++.h>
#define ll long long
using namespace std;

vector<string> vec[24];

int main()
{
    ll i,j,k;
    vec[0].push_back("artificial");
    vec[0].push_back("intelligence");

    vec[1].push_back("algorithm");

    vec[2].push_back("network");

    vec[3].push_back("database");

    vec[4].push_back("parallel computing");
    vec[4].push_back("parallel");
    vec[4].push_back("distribute");

    vec[5].push_back("hardware");
    vec[5].push_back("architecture");
    vec[6].push_back("software");

    vec[7].push_back("machine learning");
    vec[7].push_back("pattern");

    vec[8].push_back("scientific");

    vec[9].push_back("bioinformatics");
    vec[9].push_back("computational biology");
    vec[9].push_back("computation biology");

    vec[10].push_back("interaction");
    vec[10].push_back("human");

    vec[11].push_back("multimedia");

    vec[12].push_back("graphic");

    vec[13].push_back("computer vision");

    vec[14].push_back("data mining");
    //vec[14].push_back("ata Mining");

    vec[15].push_back("language");

    vec[16].push_back("security");
    vec[16].push_back("priavcy");

    vec[17].push_back("retrieval");
    vec[17].push_back("information");

    vec[18].push_back("nlp");
   // vec[18].push_back("atural Language");
    vec[18].push_back("natural language");
    vec[18].push_back("speech");



    vec[19].push_back("www");
    vec[19].push_back("wide web");
  //  vec[19].push_back("ide Web");

    vec[20].push_back("computer education");
   // vec[20].push_back("omputer Education");

   // vec[21].push_back("os");
    vec[21].push_back("operating");

    vec[22].push_back("real time");
    vec[22].push_back("embedded");

    vec[23].push_back("simulation");
    freopen("temp.txt", "r", stdin);
    freopen("maped1.txt", "w", stdout);

    string str;
    ll fl,siz;
    for (i = 0; i < 54239; i++) {
        getline(cin, str);
        std::transform(str.begin(), str.end(), str.begin(), ::tolower);
        fl=0;
        for (j = 0; j < 24; j++) {
            siz = vec[j].size();

            for (k = 0; k < siz; k++) {
                if (str.find(vec[j][k]) != string::npos) {
//                    cout << str.substr(0,8) << " " << j << endl;
                    fl=j+1;
                    break;
                }
            }
            if (fl!=0) break;
        }
       if (fl) {
            switch(fl) {
                case 1:
                    cout << str.substr(0,8) << " AI" << endl;
                    break;
                case 2:
                    cout << str.substr(0,8) << " ALGO" << endl;
                    break;
                case 3:
                    cout << str.substr(0,8) << " NW" << endl;
                    break;
                case 4:
                    cout << str.substr(0,8) << " DB" << endl;
                    break;
                    case 5:
                    cout << str.substr(0,8) << " DIST" << endl;
                    break;
                    case 6:
                    cout << str.substr(0,8) << " ARC" << endl;
                    break;
                    case 7:
                    cout << str.substr(0,8) << " SE" << endl;
                    break;
                    case 8:
                    cout << str.substr(0,8) << " ML" << endl;
                    break;
                    case 9:
                    cout << str.substr(0,8) << " SC" << endl;
                    break;
                    case 10:
                    cout << str.substr(0,8) << " BIO" << endl;
                    break;
                    case 11:
                    cout << str.substr(0,8) << " HCI" << endl;
                    break;
                    case 12:
                    cout << str.substr(0,8) << " MUL" << endl;
                    break;
                    case 13:
                    cout << str.substr(0,8) << " GRP" << endl;
                    break;
                    case 14:
                    cout << str.substr(0,8) << " CV" << endl;
                    break;
                    case 15:
                    cout << str.substr(0,8) << " DM" << endl;
                    break;
                    case 16:
                    cout << str.substr(0,8) << " PL" << endl;
                    break;
                    case 17:
                    cout << str.substr(0,8) << " SEC" << endl;
                    break;
                    case 18:
                    cout << str.substr(0,8) << " IR" << endl;
                    break;
                    case 19:
                    cout << str.substr(0,8) << " NLP" << endl;
                    break;
                    case 20:
                    cout << str.substr(0,8) << " WWW" << endl;
                    break;
                    case 21:
                    cout << str.substr(0,8) << " EDU" << endl;
                    break;
                    case 22:
                    cout << str.substr(0,8) << " OS" << endl;
                    break;
                    case 23:
                    cout << str.substr(0,8) << " RT" << endl;
                    break;
                    case 24:
                    cout << str.substr(0,8) << " SIM" << endl;

            }
        }
    }
    return 0;

}
