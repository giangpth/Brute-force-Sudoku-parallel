#include <string>
#include <iostream>

#include <ff/ff.hpp>
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>
#include <ff/dc.hpp>
#include <vector>
#include <cmath>
#include <queue> 
#include <chrono>
#include <functional>
using namespace ff;
using namespace std;


using namespace std;
using namespace std::chrono;

#define START(timename) auto timename = std::chrono::system_clock::now();
#define STOP(timename,elapsed)  auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - timename).count();

// #define DEBUG
// #define STAT
// #define RES

#define MIN_TOSEND  4

class utimer {
    std::chrono::system_clock::time_point start;
    std::chrono::system_clock::time_point stop;
    std::string message; 
    using usecs = std::chrono::microseconds;
    using msecs = std::chrono::milliseconds;

private:
    long * us_elapsed;
  
public:

    utimer(const std::string m) : message(m),us_elapsed((long *)NULL) {
        start = std::chrono::system_clock::now();
    }
    
    utimer(const std::string m, long * us) : message(m),us_elapsed(us) {
        start = std::chrono::system_clock::now();
    }

    ~utimer() {
        stop =
        std::chrono::system_clock::now();
        std::chrono::duration<double> elapsed =
        stop - start;
        auto musec =
        std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    
        #ifndef STAT
        std::cout << message << " computed in \t \t \t \t \t" << musec << " \t \t usec "  << std::endl;
        #endif

        #ifdef STAT
        std::cout << musec << "\t";
        #endif

        if(us_elapsed != NULL)
        (*us_elapsed) = musec;
  }
};

vector<short> findEmptyCells(short * board)
{
    vector<short> lEmpty;
    for(short i = 0; i < 81; i++)
    {   
        if(board[i] == 0) 
        {
            lEmpty.push_back(i);
        }
    }
    return lEmpty;
}

void printBoard(const short* board)
{
    for (short i = 0; i < 9; i++)
    {
        for(short j = 0; j < 9; j++)
        {
            cout << board[i*9+j] << " ";
        }
        cout << endl;
    }
    cout << endl;
}

bool checkRow(const short* board, short row, short cand)
{
    bool isPresent;
    isPresent = cand == board[row*9  ] || cand == board[row*9+1] || cand == board[row*9+2] ||
                cand == board[row*9+3] || cand == board[row*9+4] || cand == board[row*9+5] ||
                cand == board[row*9+6] || cand == board[row*9+7] || cand == board[row*9+8];
    return !isPresent;
}

bool checkCol(const short* board, short col, short cand)
{
    bool isPresent;
    isPresent = cand == board[    col] || cand == board[1*9+col] || cand == board[2*9+col] ||
                cand == board[3*9+col] || cand == board[4*9+col] || cand == board[5*9+col] ||
                cand == board[6*9+col] || cand == board[7*9+col] || cand == board[8*9+col];
    return !isPresent;
}

bool checkBlock(const short* board, short row, short col, short cand)
{
    int brow = row/3;
    int bcol = col/3;
    bool isPresent;
    isPresent = cand == board[(brow*3+0)*9+bcol*3+0] || cand == board[(brow*3+0)*9+bcol*3+1] || cand == board[(brow*3+0)*9+bcol*3+2] || 
                cand == board[(brow*3+1)*9+bcol*3+0] || cand == board[(brow*3+1)*9+bcol*3+1] || cand == board[(brow*3+1)*9+bcol*3+2] ||
                cand == board[(brow*3+2)*9+bcol*3+0] || cand == board[(brow*3+2)*9+bcol*3+1] || cand == board[(brow*3+2)*9+bcol*3+2];
    return !isPresent;      
}


struct node
{
    short board[81];
    short curSlot;
    node(short _board[81], short _slot)
    {
        memcpy(board, _board, sizeof(short)*81);
        curSlot = _slot;
    };
    node(){}
};

atomic <bool> ter(false);

//<IN, OUT>
struct master:ff_node_t<vector<node>, node>
{   
    node root; 
    vector<short> lEmpty;
    int nw;
    int onthefly;
    master(node _root, int _nw)
    {
        root = _root;
        nw = _nw;
        onthefly = nw;
    }

    int svc_init()
    {
        lEmpty = findEmptyCells(root.board);
        ff_send_out(new node(root.board, root.curSlot));
        return 0;
    } 
    node* svc(vector<node>* rec)
    {
        if(ter)
        {
            #ifdef DEBUG
            cout << "Master in checking ter" << endl;
            #endif
            if(onthefly == 0)
            {
                return EOS;
            }
        }
        if(rec != nullptr) // receive some thing from workers
        {
            //add to my jobs list
            vector<node> &tem = *rec;
            // cout << "Master receive " << tem.size() << " jobs" << endl;
            for(int i = 0; i < tem.size(); i++)
            {
                ff_send_out(new node(tem[i].board, tem[i].curSlot));
            }
        }
        delete rec;
        return GO_ON;
    }
    void eosnotify(ssize_t id)
    {
        onthefly--;
        #ifdef DEBUG
        cout << "Master receive EOS " << onthefly << endl;
        #endif
        for(int i = 0; i < nw; i++)
        {
            ff_send_out(EOS);
        }
    }  
    void svc_end()
    {
        #ifdef DEBUG
        cout << "Master in svc end" << endl;
        #endif
    }
};

// <IN, OUT>
struct worker:ff_node_t<node, vector<node> >
{
    node root;
    vector<short> lEmpty;
    int nw;
    worker(node _root, int _nw)
    {
        root = _root;
        nw = _nw;
    }
    int svc_init()
    {
        lEmpty = findEmptyCells(root.board);
        return 0;
    }
    vector<node>* svc(node* rec)
    {
        if(ter)
        {
            #ifdef DEBUG
            cout << "Worker " << this->get_my_id() << " in checking ter"<< endl;
            #endif
            return EOS;
        }
        if (rec != nullptr)
        {
            node& tem = *rec;
            if(tem.curSlot >= lEmpty.size())
            {
                #ifdef RES
                cout << "Fine result" << endl;
                printBoard(tem.board);
                #endif
                #ifdef DEBUG
                cout << this->get_my_id() << ": Find result from the job that master sent" << endl;
                printBoard(tem.board);
                #endif
                ff_send_out(EOS);
                ter = true;
                #ifdef DEBUG
                cout << this->get_my_id() << " sends EOS" << endl;
                #endif
                return EOS;
            }
            vector<node> * tosend = new vector<node>();
            short row = lEmpty[tem.curSlot]/9;
            short col = lEmpty[tem.curSlot]%9;

            for(int i = 1; i <= 9; i++) // first level 
            {
                if (checkRow(tem.board, row, i) &&
                    checkCol(tem.board, col, i) &&
                    checkBlock(tem.board, row, col, i))
                {
                    node node1st(tem.board, tem.curSlot+1);
                    node1st.board[lEmpty[tem.curSlot]] = i;

                    if(node1st.curSlot >= lEmpty.size())
                    {
                        #ifdef RES
                        cout << "Find result: " << endl;
                        printBoard(node1st.board);
                        #endif
                        #ifdef DEBUG
                        cout << this->get_my_id() << " finds result" << endl;
                        printBoard(node1st.board);
                        #endif

                        ff_send_out(EOS);
                        ter = true;
                        #ifdef DEBUG
                        cout << this->get_my_id() << " sends EOS" << endl;
                        #endif
                        delete tosend;
                        return EOS;
                    }

                    short row1 = lEmpty[node1st.curSlot]/9;
                    short col1 = lEmpty[node1st.curSlot]%9;

                    for (int j= 1; j <= 9; j++) //second level
                    {
                        if (checkRow(node1st.board, row1, j) &&
                            checkCol(node1st.board, col1, j) &&
                            checkBlock(node1st.board, row1, col1, j))
                        {
                            node node2st(node1st.board, node1st.curSlot+1);
                            node2st.board[lEmpty[node1st.curSlot]] = j;

                            if(node2st.curSlot >= lEmpty.size())
                            {
                                #ifdef RES
                                cout << "Find result: " << endl;
                                printBoard(node2st.board);
                                #endif
                                #ifdef DEBUG
                                cout << this->get_my_id() << " finds result" << endl;
                                printBoard(node2st.board);
                                #endif
                                ff_send_out(EOS);
                                ter = true;
                                #ifdef DEBUG
                                cout << this->get_my_id() << " sends EOS" << endl;
                                #endif
                                delete tosend;
                                return EOS;
                            }
                            // ff_send_out(new node(node2st.board, node2st.curSlot));
                            int row2 = lEmpty[node2st.curSlot]/9;
                            int col2 = lEmpty[node2st.curSlot]%9;
                            for(int k = 1; k <= 9; k++) // third level
                            {
                                if (checkRow(node2st.board, row2, k) &&
                                    checkCol(node2st.board, col2, k) &&
                                    checkBlock(node2st.board, row2, col2, k))
                                {
                                    node node3(node2st.board, node2st.curSlot+1);
                                    node3.board[lEmpty[node2st.curSlot]] = k;
                                    if(node3.curSlot >= lEmpty.size())
                                    {
                                        #ifdef RES
                                        cout << "Find result: " << endl;
                                        printBoard(node3.board);
                                        #endif

                                        #ifdef DEBUG 
                                        cout << this->get_my_id() << " find result" << endl;
                                        printBoard(node3.board);
                                        #endif
                                    
                                        ff_send_out(EOS);
                                        ter = true;
                                        delete tosend;
                                        return EOS;
                                    }
                                    tosend->push_back(node3);
                                }
                            }
                        }
                    }
                }
            }
            if (tosend->size() >= MIN_TOSEND)
            {
                ff_send_out(tosend);
            }
            else if(tosend->size() == 0)
            {
                delete tosend;
            }
            else
            {
                while(!tosend->empty())
                {
                    node cur = tosend->back();
                    tosend->pop_back();

                    if (cur.curSlot >= lEmpty.size())
                    {
                        #ifdef RES
                        cout << "Find result: " << endl;
                        printBoard(cur.board);
                        #endif

                        #ifdef DEBUG 
                        cout << this->get_my_id() << " find result" << endl;
                        printBoard(cur.board);
                        #endif
                    
                        ff_send_out(EOS);
                        ter = true;
                        return EOS;
                    }
                    short crow = lEmpty[cur.curSlot]/9;
                    short ccol = lEmpty[cur.curSlot]%9;

                    for(int i = 1; i <= 9; i++)
                    {
                        if (checkRow(cur.board, crow, i) &&
                            checkCol(cur.board, ccol, i) &&
                            checkBlock(cur.board, crow, ccol, i))
                        {
                            node nnode(cur.board, cur.curSlot+1);
                            nnode.board[lEmpty[cur.curSlot]] = i;

                            if(nnode.curSlot >= lEmpty.size())
                            {
                                #ifdef RES
                                cout << "Find result: " << endl;
                                printBoard(nnode.board);
                                #endif

                                #ifdef DEBUG
                                cout << this->get_my_id() << " find result" << endl;
                                printBoard(nnode.board);
                                #endif
                                ff_send_out(EOS);
                                ter = true;
                                return EOS;
                            }
                            tosend->push_back(nnode);
                        }

                    }
                }
                delete tosend;
            }
            delete rec;    
        }
        return GO_ON;
    }
    void eosnotify(ssize_t id)
    {
        #ifdef DEBUG
        cout << "Worker " << this->get_my_id() << " receive EOS" << endl;
        #endif
        ff_send_out(EOS);
    }
    void svc_end()
    {
        #ifdef DEBUG
        cout << "Worker " << this->get_my_id() << " end" << endl;
        #endif
    }

};

void solverff(short* board, int nw)
{
    utimer t("\t");

    node root(board, 0);
    master m(root, nw);

    vector<std::unique_ptr<ff_node> > workers;

    for(int i = 0; i < nw; i++)
    {
        workers.push_back(make_unique<worker>(root, nw));
    }
    ff_Farm<node> farm(std::move(workers), m);
    farm.remove_collector();
    farm.wrap_around();

    if(farm.run_and_wait_end() < 0)
    {
        error("Running farm fail");
        return;
    }
}



#ifdef B1
short board[81] = { 8, 0, 0, 0, 0, 0, 0, 0, 0 , 
                    0, 0, 3, 6, 0, 0, 0, 0, 0 , 
                    0, 7, 0, 0, 9, 0, 2, 0, 0 , 
                    0, 5, 0, 0, 0, 7, 0, 0, 0 , 
                    0, 0, 0, 0, 4, 5, 7, 0, 0 , 
                    0, 0, 0, 1, 0, 0, 0, 3, 0 , 
                    0, 0, 1, 0, 0, 0, 0, 6, 8 , 
                    0, 0, 8, 5, 0, 0, 0, 1, 0 , 
                    0, 9, 0, 0, 0, 0, 4, 0, 0 };
#endif

#ifdef B2
short board[81] = { 0, 5, 0, 7, 6, 0, 0, 0, 9 , 
                    0, 2, 0, 0, 0, 0, 0, 0, 0 , 
                    0, 0, 7, 9, 0, 0, 5, 0, 0 , 
                    0, 1, 0, 0, 0, 2, 0, 0, 0 , 
                    6, 0, 0, 0, 0, 1, 0, 0, 7 , 
                    8, 7, 0, 5, 0, 0, 0, 0, 3 , 
                    0, 0, 0, 3, 0, 0, 0, 4, 0 , 
                    4, 0, 0, 0, 5, 0, 8, 0, 0 , 
                    0, 0, 0, 0, 0, 0, 0, 0, 6 };
#endif

#ifdef B3
short board[81] = { 0, 0, 0, 1, 0, 0, 6, 0, 0, 
                    0, 0, 2, 5, 0, 8, 0, 0, 0, 
                    0, 5, 0, 7, 0, 0, 2, 1, 0, 
                    0, 0, 0, 0, 0, 0, 9, 0, 6, 
                    2, 0, 6, 0, 4, 0, 0, 0, 0, 
                    0, 0, 0, 0, 0, 0, 0, 0, 3, 
                    1, 9, 0, 0, 0, 7, 0, 0, 8, 
                    0, 0, 0, 0, 0, 5, 3, 0, 0, 
                    0, 0, 4, 0, 0, 0, 0, 9, 0};
#endif


#ifdef B4
short board[81] = { 4, 0, 0, 0, 0, 0, 0, 6, 0,
                    5, 0, 0, 0, 3, 2, 0, 0, 0,
                    9, 1, 0, 0, 0, 4, 0, 0, 0, 
                    0, 0, 0, 6, 0, 0, 4, 5, 0, 
                    2, 0, 0, 0, 1, 5, 0, 0, 6, 
                    0, 0, 0, 0, 0, 0, 0, 1, 0, 
                    0, 0, 8, 0, 0, 0, 0, 2, 0, 
                    0, 0, 0, 8, 0, 0, 5, 4, 0, 
                    0, 7, 0, 0, 0, 0, 1, 0, 0};
#endif 


#ifdef B5
short board[81] = { 0, 0, 2, 0, 9, 0, 6, 0, 0, 
                    0, 0, 0, 0, 4, 0, 0, 0, 3, 
                    1, 0, 0, 0, 0, 8, 0, 0, 0, 
                    7, 3, 0, 0, 0, 0, 0, 0, 2, 
                    0, 8, 0, 0, 0, 0, 4, 0, 0, 
                    0, 0, 0, 0, 0, 0, 0, 0, 8, 
                    9, 0, 0, 0, 0, 0, 0, 0, 5, 
                    0, 5, 0, 0, 3, 4, 0, 2, 0, 
                    0, 0, 0, 6, 2, 0, 0, 0, 1};
#endif 

#ifdef B6
short board[81] = { 0, 0, 0, 0, 0, 7, 0, 8, 1, 
                    5, 0, 0, 0, 0, 4, 0, 0, 0, 
                    0, 2, 0, 0, 0, 3, 0, 0, 0,
                    0, 8, 0, 0, 0, 0, 0, 7, 3, 
                    0, 6, 0, 0, 0, 0, 0, 0, 0, 
                    0, 0, 4, 5, 6, 0, 2, 0, 0, 
                    4, 0, 0, 8, 0, 0, 0, 1, 7, 
                    0, 1, 0, 0, 0, 0, 0, 0, 0, 
                    0, 0, 0, 0, 9, 0, 0, 2, 0};
#endif



int main(int argc, char** argv)
{
    if(argc < 2)
    {
        cout << "SudokuFf" << " number_of_workers" << endl;
        return -1;
    }
    int nw = atoi(argv[1]);
    if(nw < 1)
    {
        cout << "Number of workers must be greater than 0" << endl;
        return -1;
    }

    solverff(board, nw);
    
    return 0;
}

