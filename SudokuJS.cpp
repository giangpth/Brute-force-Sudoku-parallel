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

#define MAX_STACK_SIZE      4096
#define PADDING_SIZE        14 // L1 cache line = 64, size of node = 164       
#define MAX_EMPTY_COUNT     5    

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

// find list of empty cells in the board
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
    #ifdef PADDING
    short padding[PADDING_SIZE];
    #endif 
    node(short _board[81], short _slot)
    {
        memcpy(board, _board, sizeof(short)*81);
        curSlot = _slot;
    }
    node()
    {
        curSlot = -1;
    }
};


// expect to push and pop new job at the end of the stack and steal job from the begining of the stack, make a circle
struct stealingStack
{
    atomic_int stealidx;
    atomic_int njobs;

    int addidx = 0;
    int id;

    //for direct variable;
    // 1: add and pop from the right (the tail) of the array, steal from the left (the head)
    // -1: add and pop from the left (the head) of the array, steal from the right (the tail)
    // atomic<int> direct;

    node* jobs;

    stealingStack()
    {
        jobs = (node*) malloc(MAX_STACK_SIZE*sizeof(node));
        stealidx = 0; 
        njobs = 0;
    }
    void setId(int _id)
    {
        id = _id;
    }

    // check njob > 0 before calling this function 
    node pop() 
    {
        int popidx = addidx - 1;
        if (popidx < 0)
        {
            popidx = MAX_STACK_SIZE - 1;
        }
        node toreturn = jobs[popidx];
        jobs[popidx].curSlot = -1;
        addidx --;
        njobs --;


        #ifdef DEBUG
        // cout << "Queue " << id << " pops at position " << popidx << " now has " << njobs << " jobs" << endl;
        #endif

        return toreturn;
    }

    void push(node job)
    {
        while(njobs >= MAX_STACK_SIZE); 
        jobs[addidx] = job;
        njobs ++;

        #ifdef DEBUG
        // cout << "Queue " << id << " push at pos " << addidx << " now has " << njobs << " jobs" << endl;
        #endif 

        addidx ++;
        if (addidx >= MAX_STACK_SIZE)
        {
            addidx = 0;
            #ifdef DEBUG
            cout << "Queue " << id << " change pushing direction" << endl;
            #endif
        }

    }

    // to be called by other thread, need to check njob > 0 to make sure addidx != stealidx
    node steal() 
    {
        //check direction
        // auto mstealidx = stealidx.load(std::memory_order_acquire);

        node toreturn = jobs[stealidx];
        jobs[stealidx].curSlot = -1;
        njobs --;

        #ifdef DEBUG
        cout << "Queue " << id << " is stolen job at pos " << stealidx << " now has " << njobs << " jobs" << endl;
        #endif 

        stealidx ++;
        if (stealidx >= MAX_STACK_SIZE)
        {
            stealidx = 0;
            #ifdef DEBUG
            cout << "Queue " << id << " change stealing direction" << endl;
            #endif
        }
        return toreturn;
    }

    ~stealingStack()
    {
        free(jobs);
    }
};



void solverParSteal(short * board, int nw)
{
    vector<short> lEmpty = findEmptyCells(board);
    
    atomic<bool> stop(false);
    stealingStack tstacks[nw]; // each stealingStack for each worker 
    for(int i = 0; i < nw; i++)
    {
        tstacks[i].setId(i);
    }
    vector<thread> workers;

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
                    

                    tstacks[counter].push(node2nd);
                    counter ++;
                    if(counter >= nw)
                    {
                        counter = 0;
                    }
                }
            }
        }
    } 

    // thread task
    auto threadTask = [&](int id, vector<short> mEmpty)
    {
        int countempty = 0;
        while(true) 
        {
            if(stop)
            {
                #ifdef DEBUG
                cout << "Thread " << id << " receives stop signal" << endl;
                #endif
                goto terminate;
            }
            #ifdef KILL
            if(countempty >= MAX_EMPTY_COUNT)
            {
                #ifdef DEBUG
                cout << "Thread " << id << " is empty for a long time, suicide" << endl;
                #endif 
                goto terminate;
            }
            #endif

            if(tstacks[id].njobs > 0)
            {
                countempty = 0;
                node cur = tstacks[id].pop();
                if (cur.curSlot > -1)
                {
                    short row = mEmpty[cur.curSlot]/9;
                    short col = mEmpty[cur.curSlot]%9;
                    for(short i = 1; i <= 9; i++) // do the first level of cur node 
                    {
                        if (checkRow(cur.board, row, i) &&
                            checkCol(cur.board, col, i) &&
                            checkBlock(cur.board, row, col, i))
                        {
                            node node1(cur.board, cur.curSlot+1);
                            node1.board[mEmpty[cur.curSlot]] = i;
                            if(node1.curSlot >= mEmpty.size())
                            {
                                #ifdef RES
                                cout << "Thread " << id << " find result" << endl;
                                printBoard(node1.board);
                                #endif 
                                stop = true;
                                goto terminate;
                            }
                            // tstacks[id].push(node1);

                            short row1 = mEmpty[node1.curSlot]/9;
                            short col1 = mEmpty[node1.curSlot]%9;

                            for(short j = 1; j <= 9; j++) // second level 
                            {
                                if (checkRow(node1.board, row1, j) &&
                                    checkCol(node1.board, col1, j) &&
                                    checkBlock(node1.board, row1, col1, j))
                                {
                                    node node2(node1.board, node1.curSlot+1);
                                    node2.board[mEmpty[node1.curSlot]] = j;
                                    
                                    if(node2.curSlot >= mEmpty.size())
                                    {
                                        #ifdef RES 
                                        cout << "Thread " << id << " found result" << endl;
                                        printBoard(node2.board);
                                        #endif
                                        stop = true;
                                        goto terminate;
                                    }

                                    short row2 = mEmpty[node2.curSlot]/9;
                                    short col2 = mEmpty[node2.curSlot]%9;
                                    for(short k = 1; k <= 9; k++) // third level
                                    {
                                        if (checkRow(node2.board, row2, k) &&
                                            checkCol(node2.board, col2, k) &&
                                            checkBlock(node2.board, row2, col2, k))
                                        {
                                            node node3(node2.board, node2.curSlot+1);
                                            node3.board[mEmpty[node2.curSlot]] = k;

                                            if(node3.curSlot >= mEmpty.size())
                                            {
                                                #ifdef DEBUG
                                                cout << "Thread " <<  id << " found result" << endl;
                                                printBoard(node3.board);
                                                #endif
                                                stop = true;
                                                goto terminate;
                                            }
                                            tstacks[id].push(node3);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else // steal from the neighbor on the left
            {
                countempty ++;
                int stealfrom = id - 1;
                if (stealfrom < 0)
                {
                    stealfrom = nw - 1;
                }
                if (tstacks[stealfrom].njobs > 3)
                {
                    tstacks[id].push(tstacks[stealfrom].steal());
                }
            }
        }
        terminate:
        #ifdef DEBUG
        cout << "Thread " << id << " terminates";
        #endif 
        return;
    };

    int max_threads = thread::hardware_concurrency();

    for (short i = 0; i < nw; i++)
    {
        workers.push_back(thread(threadTask, i, lEmpty));
        
        #ifdef PINNING
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        int rc = pthread_setaffinity_np(workers[i].native_handle(), 
                 sizeof(cpu_set_t), &cpuset);
        #endif 
    }

    for (short i = 0; i < nw; i++)
    {
        workers[i].join();
    }
    
}

void solverSeq(short* board)
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
    // cout << "Empty stack" << endl;
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
        cout << "SudokuPar " << " nthreads" << endl;
        return -1;
    }   
    int nw = atoi(argv[1]);
    if (nw < 1)
    {
        cout << "Number of workers needs to be greater than 0" << endl;
        return -1;
    }
    #ifndef SEQ
    {
        utimer t("Running time");
        solverParSteal(board, nw);
    }
    #endif
    #ifdef SEQ
    {
        utimer t("Running time");
        solverSeq(board);
    }
    #endif

    return 0;
}

