#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <iostream>
#include <vector>
#include <math.h>
#include <chrono>
#include <thread>
#include <mutex>
#include <cstring>
#include <queue>
#include <stack>
#include <atomic>
#include <unistd.h>
#include <sys/types.h>
#include <sched.h>


using namespace std;
using namespace std::chrono;

#define START(timename) auto timename = std::chrono::system_clock::now();
#define STOP(timename,elapsed)  auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - timename).count();

// #define DEBUG
// #define STAT
// #define RES

#define MAX_EMPTY_TIME       5

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
        std::cout << message << " computed in \t \t" << musec << " \t \t usec "  << std::endl;
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

void printBoard(short* board)
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

bool checkRow(short* board, short row, short cand)
{
    bool isPresent;
    isPresent = cand == board[row*9  ] || cand == board[row*9+1] || cand == board[row*9+2] ||
                cand == board[row*9+3] || cand == board[row*9+4] || cand == board[row*9+5] ||
                cand == board[row*9+6] || cand == board[row*9+7] || cand == board[row*9+8];
    return !isPresent;
}

bool checkCol(short* board, short col, short cand)
{
    bool isPresent;
    isPresent = cand == board[    col] || cand == board[1*9+col] || cand == board[2*9+col] ||
                cand == board[3*9+col] || cand == board[4*9+col] || cand == board[5*9+col] ||
                cand == board[6*9+col] || cand == board[7*9+col] || cand == board[8*9+col];
    return !isPresent;
}

bool checkBlock(short* board, short row, short col, short cand)
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
    }
};

void solverParSteal(short* board, int nw)
{
    #ifdef DEBUG
    std::chrono::system_clock::time_point tstart;
    std::chrono::system_clock::time_point tstop;
    tstart = std::chrono::system_clock::now();
    #endif

    vector<short> lEmpty = findEmptyCells(board);
    short cutoffdepth = lEmpty.size()/5*4; // only steal job that is above the cut off depth. 

    atomic<bool> stop (false); // stop signal for all threads 

    vector<vector<node> >tarrays(nw); //each stack for each worker
    vector<mutex> tlock(nw); // lock for worker's stack 
    vector<thread> workers;

    //do the first 2 levels to give each stack some jobs
    int counter = 0;
    node root(board, 0);

    short row = lEmpty[root.curSlot]/9;
    short col = lEmpty[root.curSlot]%9;
    for(int i = 1; i<= 9; i++) // first level
    {
        if (checkRow(root.board, row, i) &&
            checkCol(root.board, col, i) &&
            checkBlock(root.board, row, col, i))
        {
            node node1st(root.board, root.curSlot+1);
            node1st.board[lEmpty[root.curSlot]] = i;
            if(node1st.curSlot >= lEmpty.size())
            {
                #ifdef RES
                cout << "Find result: " << endl;
                printBoard(node1st.board);
                #endif
                return;
            }

            short nrow = lEmpty[node1st.curSlot]/9;
            short ncol = lEmpty[node1st.curSlot]%9;

            for(int j = 1; j <= 9; j++) // second level
            {
                if (checkRow(node1st.board, nrow, j) &&
                    checkCol(node1st.board, ncol, j) &&
                    checkBlock(node1st.board, nrow, ncol, j))
                {
                    node node2nd(node1st.board, node1st.curSlot+1);
                    node2nd.board[lEmpty[node1st.curSlot]] = j;
                    if(node1st.curSlot >= lEmpty.size())
                    {
                        #ifdef RES
                        cout << "Find result: " << endl;
                        printBoard(node1st.board);
                        #endif
                        return;
                    }
                    

                    tarrays[counter].push_back(node2nd);
                    counter ++;
                    if(counter >= nw)
                    {
                        counter = 0;
                    }
                }
            }
        }
    }
#ifdef DEBUG
    for(int i = 0; i < tarrays.size(); i++)
    {
        cout << "Vector " << i << " has size " << tarrays[i].size() << endl;
    }
    tstop = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed = tstop - tstart;
    auto musec = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    std::cout << "Sequential part computed in \t \t" << musec << " usec " << std::endl;
#endif

    auto threadTask = [&](short idx)
    {
        short emptycount = 0; //thread will be kill after a number of continuous time being empty
        vector<short> myEmptyList(lEmpty);
        bool mystop = false;
        while(true)
        {
            if(stop)
            {
                #ifdef DEBUG
                cout << "Thread " << idx << " receives stop signal" << endl;
                #endif 
                goto terminate;
            }

            // tlock[idx].lock();
            while(!tlock[idx].try_lock());
            bool isempty = tarrays[idx].empty();
            tlock[idx].unlock();

            if (isempty) // do job stealing 
            {
                emptycount ++;
                if(emptycount >= MAX_EMPTY_TIME)
                {
                    #ifdef DEBUG
                    cout << "Thread " << idx << " is empty for too long, suicide" << endl;
                    #endif
                    break;
                }
                #ifdef DEBUG
                cout << "Thread " << idx << " is empty. " << "Do steal jobs" << endl;
                #endif

                int stealidx = idx - 1; // steal of the left neighbor
                if (stealidx < 0)
                {
                    stealidx = nw - 1;
                }
                if (stealidx == idx) break;

                while(!tlock[stealidx].try_lock()); 
                while(!tlock[idx].try_lock());
                
                int numjob = tarrays[stealidx].size()/2;
                for(int i = 0; i < numjob; i++)
                {
                    if (tarrays[stealidx].front().curSlot <= cutoffdepth) //only steal job that is above the cut off depth 
                    {
                        tarrays[idx].push_back(tarrays[stealidx].front());
                        tarrays[stealidx].erase(tarrays[stealidx].begin());
                    }
                    else
                    {
                        break;
                    }
                }
                #ifdef DEBUG
                cout << "Thread " << stealidx << " has " << tarrays[stealidx].size() << endl;
                cout << "Thread " << idx << " has " << tarrays[idx].size() << endl;
                #endif
                tlock[stealidx].unlock(); tlock[idx].unlock();
            }
            //do my own job
            // tlock[idx].lock();
            while(!tlock[idx].try_lock());

            if (tarrays[idx].empty())
            {
                tlock[idx].unlock();
                continue;
            }
            else
            { 
                node cur = tarrays[idx].back(); //get the last element because of dfs
                tarrays[idx].pop_back();
                tlock[idx].unlock(); 

                short row = myEmptyList[cur.curSlot]/9;
                short col = myEmptyList[cur.curSlot]%9;

                for(short i = 1; i <= 9; i++) // do the first level of cur node 
                {
                    if (checkRow(cur.board, row, i) &&
                        checkCol(cur.board, col, i) &&
                        checkBlock(cur.board, row, col, i))
                    {
                        node node1(cur.board, cur.curSlot+1);
                        node1.board[myEmptyList[cur.curSlot]] = i;
                        if(node1.curSlot >= myEmptyList.size())
                        {
                            #ifdef RES
                            cout << "Thread " << idx << " find result" << endl;
                            printBoard(node1.board);
                            #endif 
                            stop = true;
                            mystop = true;
                            goto terminate;
                        }
                        
                        short row1 = myEmptyList[node1.curSlot]/9;
                        short col1 = myEmptyList[node1.curSlot]%9;

                        for (short j = 1; j <= 9; j++) // do the second level
                        {
                            if (checkRow(node1.board, row1, j) &&
                                checkCol(node1.board, col1, j) &&
                                checkBlock(node1.board, row1, col1, j))
                            {   
                                node node2(node1.board, node1.curSlot+1);
                                node2.board[myEmptyList[node1.curSlot]] = j;
                                if(node2.curSlot >= myEmptyList.size())
                                {
                                    #ifdef RES
                                    cout << "Thread " << idx << " find result" << endl;
                                    printBoard(node2.board);
                                    #endif 
                                    stop = true;
                                    mystop = true;
                                    goto terminate;
                                }

                                short row2 = myEmptyList[node2.curSlot]/9;
                                short col2 = myEmptyList[node2.curSlot]%9;
                                // tlock[idx].lock();
                                while(!tlock[idx].try_lock());
                                for(short k = 1; k <= 9; k++) // do the third level 
                                {
                                    if (checkRow(node2.board, row2, k) &&
                                        checkCol(node2.board, col2, k) &&
                                        checkBlock(node2.board, row2, col2, k))
                                    {
                                        node node3(node2.board, node2.curSlot+1);
                                        node3.board[myEmptyList[node2.curSlot]] = k;

                                        if(node3.curSlot >= myEmptyList.size())
                                        {
                                            #ifdef RES
                                            cout << "Thread " << idx << " find result" << endl;
                                            printBoard(node3.board);
                                            #endif 
                                            tlock[idx].unlock();
                                            stop = true;
                                            mystop = true;
                                            goto terminate;
                                        }
                                        
                                        // tlock[idx].lock();
                                        tarrays[idx].push_back(node3);
                                        // tlock[idx].unlock();
                                    }
                                }
                                tlock[idx].unlock();
                            }
                        }
                    }
                }
            }
        }
        terminate: 
        #ifdef DEBUG
        cout << "Thread " << idx << " terminate" << endl;
        #endif
        return;
    };

    int max_threads = thread::hardware_concurrency();

    for (short i = 0; i < nw; i++)
    {
        workers.push_back(thread(threadTask, i));
    }
    // pin threads to cores
    #ifdef PINNING
    for(int i = 0; i < nw; i++)
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        int rc = pthread_setaffinity_np(workers[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    }
    #endif
    for (short i = 0; i < nw; i++)
    {
        workers[i].join();
    }
    
}


void solver(short* board)
{
    vector<short> lEmpty = findEmptyCells(board);
    stack<node> stateTree;

    node root(board, 0);
    stateTree.push(root);

    while(!stateTree.empty())
    {
        node cur = stateTree.top();
        stateTree.pop();
        if (cur.curSlot >= lEmpty.size())
        {
            cout << "Find result: " << endl;
            printBoard(cur.board);
            return;
        }
        short row = lEmpty[cur.curSlot]/9;
        short col = lEmpty[cur.curSlot]%9;
        for(short i = 1; i <= 9; i++)
        {
            if(checkRow(cur.board, row, i) && checkCol(cur.board, col, i) && checkBlock(cur.board, row, col, i))
            {   
                node nnode(cur.board, cur.curSlot+1);
                nnode.board[lEmpty[cur.curSlot]] = i;
                if (nnode.curSlot >= lEmpty.size())
                {
                    #ifdef RES
                    cout << "Find result: " << endl;
                    printBoard(nnode.board);
                    #endif
                    return;
                }
                
                stateTree.push(nnode);
            }
        }
    }
    cout << "Empty stack" << endl;
}



void solverPar(short* board, int nw)
{
    utimer t("Par");

    solverParSteal(board, nw);
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

int main(int argc, char** argv)
{
    if(argc < 2)
    {
        cout << "SudokuPar " << " nthreads" << endl;
        return -1;
    }   
    int nw = atoi(argv[1]);
    if (nw < 1)
    {
        cout << "Number of workers needs to be greater than 0" << endl;
        return -1;
    }
    solverPar(board, nw);
    return 0;
}

